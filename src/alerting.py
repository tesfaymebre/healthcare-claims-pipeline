"""
Alerting for the healthcare claims pipeline.

Two kinds of alert:

Failure alerts   A run failed. Raised by the orchestrator (the CLI wrapper
                 in pipeline.py, or the Airflow failure callback), because
                 only it knows the run will not be retried again.
Quality alerts   The run succeeded but the data looks wrong: too many
                 rejections or duplicates, a sudden volume drop, rejected
                 files. Evaluated from the run's own statistics against
                 thresholds in settings.yaml.

Every alert is logged and stored in pipeline_alerts, whatever its channels.
Webhook (Slack-compatible) and email are opt-in. Delivery is best effort:
a broken notification channel must never fail or mask a pipeline run, so
channel errors are logged and swallowed.

Alerts never include claim-level values: a payload sent to a chat channel
or mailbox leaves the controlled environment, so it carries counts, codes
and run ids only.
"""

from __future__ import annotations

import json
import logging
import smtplib
import urllib.request
from dataclasses import asdict, dataclass, field
from email.message import EmailMessage
from enum import IntEnum
from typing import Any, Protocol

from sqlalchemy import text

from .config import Settings, get_settings
from .database import transaction


logger = logging.getLogger("healthcare_claims_pipeline.alerting")


class Severity(IntEnum):
    INFO = 10
    WARNING = 20
    CRITICAL = 30


@dataclass
class Alert:
    code: str
    severity: Severity
    title: str
    message: str
    run_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["severity"] = self.severity.name
        return data


# Channels

class Notifier(Protocol):
    name: str

    def send(self, alert: Alert) -> None: ...


class LogNotifier:
    name = "log"

    _LEVELS = {
        Severity.INFO: logging.INFO,
        Severity.WARNING: logging.WARNING,
        Severity.CRITICAL: logging.ERROR,
    }

    def send(self, alert: Alert) -> None:
        logger.log(
            self._LEVELS[alert.severity],
            "ALERT [%s] %s: %s (run %s)",
            alert.severity.name,
            alert.code,
            alert.message,
            alert.run_id,
        )


class WebhookNotifier:
    """
    POST a JSON payload to a webhook.

    The "text" field makes the payload a valid Slack/Mattermost/Teams-workflow
    incoming-webhook message; the structured fields let any other receiver
    parse it.
    """

    name = "webhook"

    _ICONS = {Severity.INFO: "ℹ️", Severity.WARNING: "⚠️", Severity.CRITICAL: "🚨"}

    def __init__(self, url: str, timeout: float = 5.0) -> None:
        self.url = url
        self.timeout = timeout

    def payload(self, alert: Alert) -> dict[str, Any]:
        return {
            "text": (
                f"{self._ICONS[alert.severity]} *{alert.title}*\n"
                f"{alert.message}\n"
                f"`code={alert.code}` `run={alert.run_id}`"
            ),
            "alert": alert.to_dict(),
        }

    def send(self, alert: Alert) -> None:
        request = urllib.request.Request(
            self.url,
            data=json.dumps(self.payload(alert), default=str).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout):
            pass


class EmailNotifier:
    name = "email"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def send(self, alert: Alert) -> None:
        s = self.settings
        message = EmailMessage()
        message["Subject"] = f"[{alert.severity.name}] {s.pipeline_name}: {alert.title}"
        message["From"] = s.alert_email_from
        message["To"] = ", ".join(s.alert_email_to)
        message.set_content(
            f"{alert.message}\n\n"
            f"Code:   {alert.code}\n"
            f"Run id: {alert.run_id}\n\n"
            f"Details:\n{json.dumps(alert.details, indent=2, default=str)}\n"
        )
        with smtplib.SMTP(s.smtp_host, s.smtp_port, timeout=10) as smtp:
            smtp.starttls()
            if s.smtp_user:
                smtp.login(s.smtp_user, s.smtp_password or "")
            smtp.send_message(message)


def build_notifiers(settings: Settings) -> list[Notifier]:
    notifiers: list[Notifier] = [LogNotifier()]

    if settings.alert_webhook_enabled:
        if settings.alert_webhook_url:
            notifiers.append(WebhookNotifier(settings.alert_webhook_url))
        else:
            logger.warning("Webhook alerts enabled but ALERT_WEBHOOK_URL is not set")

    if settings.alert_email_enabled:
        if settings.smtp_host and settings.alert_email_from and settings.alert_email_to:
            notifiers.append(EmailNotifier(settings))
        else:
            logger.warning("Email alerts enabled but SMTP_HOST / ALERT_EMAIL_FROM / ALERT_EMAIL_TO are not all set")

    return notifiers


# Dispatch

INSERT_ALERT_SQL = """
INSERT INTO pipeline_alerts (
    pipeline_run_id, alert_code, severity, title, message, details, channels_delivered
) VALUES (
    :run_id, :code, :severity, :title, :message, CAST(:details AS JSONB), :channels
)
"""


class AlertManager:

    def __init__(
        self,
        settings: Settings | None = None,
        notifiers: list[Notifier] | None = None,
        persist: bool = True,
    ) -> None:
        self.settings = settings or get_settings()
        self.notifiers = notifiers if notifiers is not None else build_notifiers(self.settings)
        self.persist = persist
        self.min_severity = Severity[self.settings.alert_min_severity]

    def dispatch(self, alerts: list[Alert]) -> list[Alert]:
        """
        Send and store alerts at or above min_severity. Returns those sent.
        """

        if not self.settings.alerting_enabled:
            return []

        sent = [alert for alert in alerts if alert.severity >= self.min_severity]

        for alert in sent:

            delivered = []

            for notifier in self.notifiers:
                try:
                    notifier.send(alert)
                    delivered.append(notifier.name)
                except Exception as exc:  # noqa: BLE001 - never let alerting fail the run
                    logger.error("Alert channel %s failed for %s: %s", notifier.name, alert.code, exc)

            if self.persist:
                self._store(alert, delivered)

        return sent

    def _store(self, alert: Alert, delivered: list[str]) -> None:
        # Separate, short transaction: a failure alert is often raised
        # *because* the run's transaction rolled back, and it must survive.
        try:
            with transaction() as connection:
                connection.execute(
                    text(INSERT_ALERT_SQL),
                    {
                        "run_id": alert.run_id,
                        "code": alert.code,
                        "severity": alert.severity.name,
                        "title": alert.title,
                        "message": alert.message,
                        "details": json.dumps(alert.details, default=str),
                        "channels": delivered,
                    },
                )
        except Exception as exc:  # noqa: BLE001 - the database may be the thing that is down
            logger.error("Could not store alert %s: %s", alert.code, exc)


# Alert rules

def summarize_error(error: BaseException | str) -> str:
    """
    First, human-readable part of an error message.

    SQLAlchemy appends the failing statement ("[SQL: ...]") and a docs link;
    those are noise in a chat channel and the statement text can be long.
    """

    message = str(error)
    for marker in ("\n[SQL:", "\n(Background on this error"):
        message = message.split(marker, 1)[0]
    return message.strip()


def failure_alert(run_id: str | None, error: BaseException | str, stage: str | None = None) -> Alert:
    message = summarize_error(error)
    return Alert(
        code="PIPELINE_FAILED",
        severity=Severity.CRITICAL,
        title="Claims pipeline run failed",
        message=(f"Stage '{stage}' failed: " if stage else "") + message[:1000],
        run_id=run_id,
        details={
            "stage": stage,
            "error_type": type(error).__name__ if isinstance(error, BaseException) else "Error",
        },
    )


def evaluate_quality_alerts(
    stats: dict[str, Any],
    settings: Settings,
    recent_volumes: list[int] | None = None,
) -> list[Alert]:
    """
    Turn one successful run's statistics into quality alerts.

    stats uses the keys produced by pipeline.process_run(): records_received,
    records_rejected, records_flagged_duplicate, files_rejected, and
    rejected_files (names).
    """

    run_id = stats.get("run_id")
    alerts: list[Alert] = []

    received = int(stats.get("records_received") or 0)
    rejected = int(stats.get("records_rejected") or 0)
    duplicates = int(stats.get("records_flagged_duplicate") or 0)

    if received:

        rejection_rate = rejected / received

        if rejection_rate > settings.max_rejection_rate:
            top = sorted((stats.get("rule_hits") or {}).items(), key=lambda kv: -kv[1])[:5]
            alerts.append(
                Alert(
                    code="HIGH_REJECTION_RATE",
                    severity=Severity.WARNING,
                    title="Rejection rate above threshold",
                    message=(
                        f"{rejected:,} of {received:,} records rejected "
                        f"({rejection_rate:.1%}, threshold {settings.max_rejection_rate:.0%})."
                    ),
                    run_id=run_id,
                    details={"rejection_rate": round(rejection_rate, 4), "top_rules": dict(top)},
                )
            )

        duplicate_rate = duplicates / received

        if duplicate_rate > settings.max_duplicate_rate:
            alerts.append(
                Alert(
                    code="HIGH_DUPLICATE_RATE",
                    severity=Severity.WARNING,
                    title="Possible-duplicate rate above threshold",
                    message=(
                        f"{duplicates:,} of {received:,} records share a duplicate business key "
                        f"({duplicate_rate:.1%}, threshold {settings.max_duplicate_rate:.0%})."
                    ),
                    run_id=run_id,
                    details={"duplicate_rate": round(duplicate_rate, 4)},
                )
            )

    if recent_volumes:
        average = sum(recent_volumes) / len(recent_volumes)
        if average > 0 and received < settings.min_volume_ratio * average:
            alerts.append(
                Alert(
                    code="LOW_VOLUME",
                    severity=Severity.WARNING,
                    title="Record volume dropped",
                    message=(
                        f"Received {received:,} records against a recent average of {average:,.0f} "
                        f"(below {settings.min_volume_ratio:.0%})."
                    ),
                    run_id=run_id,
                    details={"recent_volumes": recent_volumes, "average": round(average, 1)},
                )
            )

    if stats.get("files_rejected"):
        alerts.append(
            Alert(
                code="FILES_REJECTED",
                severity=Severity.WARNING,
                title="Input files rejected",
                message=(
                    f"{stats['files_rejected']} file(s) were empty, unreadable or off-contract "
                    "and were moved to the rejected folder."
                ),
                run_id=run_id,
                details={"files": stats.get("rejected_files", [])},
            )
        )

    return alerts
