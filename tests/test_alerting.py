"""Unit tests for alert rules and dispatch (no database)."""

import json

from src.alerting import (
    Alert,
    AlertManager,
    Severity,
    WebhookNotifier,
    evaluate_quality_alerts,
    failure_alert,
    summarize_error,
)


class RecordingNotifier:
    name = "recording"

    def __init__(self):
        self.sent = []

    def send(self, alert):
        self.sent.append(alert)


class BrokenNotifier:
    name = "broken"

    def send(self, alert):
        raise ConnectionError("channel down")


def stats(**overrides):
    base = {
        "run_id": "run-1",
        "records_received": 1000,
        "records_rejected": 50,
        "records_flagged_duplicate": 20,
        "files_rejected": 0,
        "rule_hits": {"MISSING_REQUIRED_FIELD": 50},
    }
    return {**base, **overrides}


def codes(alerts):
    return {alert.code for alert in alerts}


def test_healthy_run_raises_no_alerts(settings):
    assert evaluate_quality_alerts(stats(), settings, recent_volumes=[1000, 1100]) == []


def test_high_rejection_rate(settings):
    settings.max_rejection_rate = 0.10
    alerts = evaluate_quality_alerts(stats(records_rejected=200), settings)
    assert codes(alerts) == {"HIGH_REJECTION_RATE"}
    assert alerts[0].details["rejection_rate"] == 0.2


def test_high_duplicate_rate(settings):
    settings.max_duplicate_rate = 0.05
    assert codes(evaluate_quality_alerts(stats(records_flagged_duplicate=100), settings)) == {"HIGH_DUPLICATE_RATE"}


def test_low_volume_against_recent_runs(settings):
    settings.min_volume_ratio = 0.5
    alerts = evaluate_quality_alerts(stats(records_received=400, records_rejected=0), settings, [1000, 1000])
    assert codes(alerts) == {"LOW_VOLUME"}


def test_no_volume_alert_without_history(settings):
    assert evaluate_quality_alerts(stats(records_received=1, records_rejected=0, records_flagged_duplicate=0), settings, []) == []


def test_rejected_files_alert(settings):
    alerts = evaluate_quality_alerts(stats(files_rejected=1, rejected_files=["bad.csv"]), settings)
    assert codes(alerts) == {"FILES_REJECTED"}
    assert alerts[0].details["files"] == ["bad.csv"]


def test_manager_delivers_to_every_channel(settings):
    recorder = RecordingNotifier()
    manager = AlertManager(settings, notifiers=[recorder], persist=False)

    sent = manager.dispatch([failure_alert("run-1", RuntimeError("boom"))])

    assert len(sent) == 1
    assert recorder.sent[0].severity is Severity.CRITICAL


def test_broken_channel_does_not_raise_or_block_others(settings):
    recorder = RecordingNotifier()
    manager = AlertManager(settings, notifiers=[BrokenNotifier(), recorder], persist=False)

    manager.dispatch([failure_alert("run-1", "boom")])

    assert len(recorder.sent) == 1


def test_min_severity_filters_alerts(settings):
    settings.alert_min_severity = "CRITICAL"
    recorder = RecordingNotifier()
    manager = AlertManager(settings, notifiers=[recorder], persist=False)

    manager.dispatch([Alert("X", Severity.WARNING, "t", "m"), failure_alert("r", "boom")])

    assert [alert.code for alert in recorder.sent] == ["PIPELINE_FAILED"]


def test_alerting_can_be_disabled(settings):
    settings.alerting_enabled = False
    recorder = RecordingNotifier()
    AlertManager(settings, notifiers=[recorder], persist=False).dispatch([failure_alert("r", "boom")])
    assert recorder.sent == []


def test_webhook_payload_is_slack_compatible_and_json():
    payload = WebhookNotifier("http://example.invalid").payload(failure_alert("run-9", "boom", stage="process"))
    assert "run-9" in payload["text"]
    assert payload["alert"]["severity"] == "CRITICAL"
    json.dumps(payload)


def test_error_summary_drops_sql_text():
    error = "(psycopg.errors.X) value too long\n[SQL: INSERT INTO t VALUES (%(a)s)]\n(Background on this error at: x)"
    assert summarize_error(error) == "(psycopg.errors.X) value too long"
    assert "[SQL" not in failure_alert("r", error).message
