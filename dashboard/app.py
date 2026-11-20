"""
Streamlit dashboard for the healthcare claims pipeline.

Run from the project root:
    streamlit run dashboard/app.py

Reads through src/analytics.py, the same query layer as the REST API, so a
number on this page always matches the API. Queries are cached for a short
TTL; the "Refresh data" button clears the cache.
"""

from __future__ import annotations

import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

# Streamlit puts this file's folder on sys.path, not the project root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import analytics  # noqa: E402
from src.analytics import ClaimFilters  # noqa: E402
from src.config import get_settings  # noqa: E402
from src.database import get_engine  # noqa: E402
from src.lineage import trace_claim  # noqa: E402
from src.rules import describe_rules, get_rule_set  # noqa: E402

CACHE_TTL_SECONDS = 60

st.set_page_config(page_title="Claims Pipeline", page_icon="🩺", layout="wide")


# Theme-aware chart colors (reference palette: one hue for magnitude,
# reserved status colors that always travel with an icon and a label).

def _is_dark() -> bool:
    try:
        return st.context.theme.type == "dark"
    except Exception:  # older Streamlit
        return False


SERIES = "#3987e5" if _is_dark() else "#2a78d6"

STATUS_LABELS = {
    "SUCCESS": "✅ Success",
    "PARTIAL": "⚠️ Partial",
    "SKIPPED": "⏭️ Skipped",
    "RUNNING": "🔄 Running",
    "FAILED": "🛑 Failed",
}

SEVERITY_LABELS = {"INFO": "ℹ️ Info", "WARNING": "⚠️ Warning", "CRITICAL": "🚨 Critical"}


# Data access (cached)

def _frame(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    for column in frame.columns:
        values = frame[column]
        if values.map(lambda v: isinstance(v, Decimal)).any():
            frame[column] = pd.to_numeric(values, errors="coerce")
        elif values.map(lambda v: type(v) is date).any():
            # Plain date objects are not JSON-serializable for Vega-Lite.
            frame[column] = pd.to_datetime(values)
    return frame


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def query(name: str, filters: ClaimFilters | None = None, **kwargs):
    function = getattr(analytics, name)
    with get_engine().connect() as connection:
        connection = connection.execution_options(postgresql_readonly=True)
        if filters is None:
            return function(connection, **kwargs)
        return function(connection, filters, **kwargs)


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def lineage(claim_id: str) -> dict:
    with get_engine().connect() as connection:
        return trace_claim(connection.execution_options(postgresql_readonly=True), claim_id)


# Chart helpers: thin marks, rounded data ends, recessive axes, tooltips.

def bar_chart(frame: pd.DataFrame, category: str, value: str, value_title: str, money: bool = False):
    fmt = "$,.0f" if money else ",.0f"
    return (
        alt.Chart(frame)
        .mark_bar(color=SERIES, cornerRadiusEnd=4, height={"band": 0.7})
        .encode(
            y=alt.Y(f"{category}:N", sort="-x", title=None),
            x=alt.X(f"{value}:Q", title=value_title, axis=alt.Axis(format=fmt, grid=True, gridOpacity=0.3)),
            tooltip=[alt.Tooltip(f"{category}:N"), alt.Tooltip(f"{value}:Q", title=value_title, format=fmt)],
        )
        .properties(height=max(160, 34 * len(frame)))
    )


def line_chart(frame: pd.DataFrame, x: str, y: str, y_title: str, money: bool = False, percent: bool = False):
    fmt = "$,.0f" if money else (".1f" if percent else ",.0f")
    hover = alt.selection_point(fields=[x], nearest=True, on="pointerover", empty=False)
    base = alt.Chart(frame).encode(
        x=alt.X(f"{x}:T", title=None, axis=alt.Axis(grid=False, format="%b %Y", labelOverlap="greedy", tickCount=8)),
        y=alt.Y(f"{y}:Q", title=y_title, axis=alt.Axis(format=fmt, gridOpacity=0.3)),
    )
    line = base.mark_line(color=SERIES, strokeWidth=2)
    points = base.mark_point(color=SERIES, filled=True, size=70).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip(f"{x}:T", title="Month", format="%b %Y"), alt.Tooltip(f"{y}:Q", title=y_title, format=fmt)],
    ).add_params(hover)
    rule = base.mark_rule(color="gray", opacity=0.4).encode(
        opacity=alt.condition(hover, alt.value(0.4), alt.value(0))
    ).transform_filter(hover)
    return (line + points + rule).properties(height=260)


def money(value) -> str:
    return f"${float(value or 0):,.0f}"


def count(value) -> str:
    """Format a count that may be NULL (e.g. on a failed run)."""
    return "–" if value is None or pd.isna(value) else f"{int(value):,}"


def percent(value) -> str:
    return "–" if value is None else f"{float(value):.1f}%"


# Sidebar filters

settings = get_settings()

with st.sidebar:
    st.header("Filters")

    try:
        options = query("filter_options")
    except Exception as exc:  # noqa: BLE001
        st.error(f"Cannot reach the database: {exc.__class__.__name__}")
        st.stop()

    min_date = options["min_service_date"] or date(2020, 1, 1)
    max_date = options["max_service_date"] or date.today()

    date_range = st.date_input(
        "Service date",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    claim_type = st.selectbox("Claim type", ["All", *options["claim_types"]])
    insurance_plan = st.selectbox("Insurance plan", ["All", *options["insurance_plans"]])
    claim_status = st.selectbox("Claim status", ["All", *options["claim_statuses"]])

    if st.button("Refresh data", width="stretch"):
        st.cache_data.clear()
        st.rerun()

    st.caption(f"Cached for {CACHE_TTL_SECONDS}s · pipeline v{settings.pipeline_version}")

start, end = (date_range + (None, None))[:2] if isinstance(date_range, tuple) else (date_range, None)

filters = ClaimFilters(
    service_date_from=start,
    service_date_to=end,
    claim_type=None if claim_type == "All" else claim_type,
    insurance_plan=None if insurance_plan == "All" else insurance_plan,
    claim_status=None if claim_status == "All" else claim_status,
)


st.title("Healthcare Claims")

overview, providers_tab, quality_tab, runs_tab, lineage_tab = st.tabs(
    ["Overview", "Providers & denials", "Data quality", "Pipeline runs", "Lineage"]
)


# Overview

with overview:
    kpi = query("kpis", filters)

    if not kpi["total_claims"]:
        st.info("No claims match these filters.")
    else:
        row1 = st.columns(4)
        row1[0].metric("Claims", f"{kpi['total_claims']:,}")
        row1[1].metric("Billed", money(kpi["total_billed"]))
        row1[2].metric("Paid", money(kpi["total_paid"]))
        row1[3].metric("Unpaid (approved − paid)", money(kpi["total_unpaid"]))

        row2 = st.columns(4)
        row2[0].metric("Approval rate", percent(kpi["approval_rate_percent"]))
        row2[1].metric("Denial rate", percent(kpi["denial_rate_percent"]))
        row2[2].metric("Avg processing days", f"{float(kpi['avg_processing_days'] or 0):.1f}")
        row2[3].metric("Possible duplicates", f"{kpi['possible_duplicates']:,}")

        if kpi["data_as_of"]:
            st.caption(f"Data as of {kpi['data_as_of']:%Y-%m-%d %H:%M} (last fact-table update)")

        monthly = _frame(query("monthly_trends", filters))

        st.caption("Months are by service date. The first and last months can be partial.")

        left, right = st.columns(2)
        with left:
            st.subheader("Monthly claim volume")
            st.altair_chart(line_chart(monthly, "service_month", "claim_count", "Claims"), width="stretch")
        with right:
            st.subheader("Monthly paid amount")
            st.altair_chart(
                line_chart(monthly, "service_month", "total_paid", "Paid", money=True),
                width="stretch",
            )

        left, right = st.columns(2)
        with left:
            st.subheader("Claims by status")
            by_status = _frame(query("claims_by_status", filters))
            st.altair_chart(bar_chart(by_status, "claim_status", "claim_count", "Claims"), width="stretch")
            with st.expander("Table: amounts by status"):
                st.dataframe(by_status, hide_index=True, width="stretch")
        with right:
            st.subheader("Average processing days by claim type")
            processing = _frame(query("processing_time_by_type", filters))
            st.altair_chart(
                bar_chart(processing, "claim_type", "avg_processing_days", "Avg days"),
                width="stretch",
            )
            with st.expander("Table: processing time"):
                st.dataframe(processing, hide_index=True, width="stretch")


# Providers & denials

with providers_tab:
    left, right = st.columns(2)

    with left:
        st.subheader("Top denial reasons")
        denials = _frame(query("top_denial_reasons", filters, limit=10))
        if denials.empty:
            st.info("No denied claims match these filters.")
        else:
            st.altair_chart(bar_chart(denials, "denial_reason", "denial_count", "Denied claims"), width="stretch")

    with right:
        st.subheader("Top providers")
        order_by = st.radio("Rank by", ["billed", "paid", "claims"], horizontal=True)
        providers = _frame(query("top_providers", filters, limit=15, order_by=order_by))
        st.dataframe(
            providers,
            hide_index=True,
            width="stretch",
            column_config={
                "total_billed": st.column_config.NumberColumn("Billed", format="$%.2f"),
                "total_paid": st.column_config.NumberColumn("Paid", format="$%.2f"),
                "denial_rate_percent": st.column_config.NumberColumn("Denial rate", format="%.1f%%"),
            },
        )

    st.subheader("High-value claims requiring review")
    high_value = query("high_value_claims", filters, limit=100)
    st.caption(f"{high_value['total']:,} claims above ${settings.high_value_threshold:,.0f}")
    st.dataframe(_frame(high_value["items"]), hide_index=True, width="stretch")


# Data quality

with quality_tab:
    st.caption("Pipeline-level view; the sidebar filters apply to the fact table only.")

    trend = _frame(query("quality_trend", limit=30))
    left, right = st.columns(2)

    with left:
        st.subheader("Rejection codes (all runs)")
        codes = _frame(query("rejection_reasons"))
        if codes.empty:
            st.info("No rejected records yet.")
        else:
            st.altair_chart(bar_chart(codes, "rejection_code", "record_count", "Records"), width="stretch")
            st.caption("A record failing several rules counts under each code.")

    with right:
        st.subheader("Rejection rate per run")
        if len(trend) < 2:
            st.info("Needs at least two completed runs.")
        else:
            st.altair_chart(
                line_chart(trend, "run_start_time", "rejection_rate_percent", "Rejected %", percent=True),
                width="stretch",
            )
            st.caption(f"Alert threshold: {settings.max_rejection_rate:.0%}")

    st.subheader("Possible duplicate groups")
    duplicates = query("duplicate_groups", filters, limit=100)
    st.caption(f"{duplicates['total']:,} groups share patient, provider, service date, procedure and amount")
    st.dataframe(_frame(duplicates["items"]), hide_index=True, width="stretch")

    st.subheader("Recent rejected records")
    code_filter = st.selectbox("Rejection code", ["All", *codes.get("rejection_code", pd.Series(dtype=str)).tolist()])
    rejected = query(
        "rejected_records",
        rejection_code=None if code_filter == "All" else code_filter,
        limit=100,
    )
    st.dataframe(
        _frame(rejected["items"]).drop(columns=["raw_record"], errors="ignore"),
        hide_index=True,
        width="stretch",
    )

    with st.expander("Active validation rules (config/validation_rules.yaml)"):
        st.dataframe(
            pd.DataFrame(describe_rules(get_rule_set(settings.validation_rules_file))),
            hide_index=True,
            width="stretch",
        )


# Pipeline runs

with runs_tab:
    runs = _frame(query("pipeline_runs", limit=50))

    if runs.empty:
        st.info("No pipeline runs recorded yet. Run: python -m src.pipeline")
    else:
        latest = runs.iloc[0]
        cols = st.columns(4)
        cols[0].metric("Last run", STATUS_LABELS.get(latest["run_status"], latest["run_status"]))
        cols[1].metric("Records received", count(latest["records_received"]))
        cols[2].metric("Records rejected", count(latest["records_rejected"]))
        failed_recent = int((runs.head(10)["run_status"] == "FAILED").sum())
        cols[3].metric("Failures in last 10 runs", failed_recent)

        display = runs.assign(status=runs["run_status"].map(lambda s: STATUS_LABELS.get(s, s)))
        st.dataframe(
            display[
                [
                    "status", "run_start_time", "duration_seconds", "files_received", "files_rejected",
                    "files_skipped", "records_received", "records_rejected", "records_inserted",
                    "records_updated", "records_unchanged", "error_message", "pipeline_run_id",
                ]
            ],
            hide_index=True,
            width="stretch",
        )

        run_id = st.selectbox("Run details", runs["pipeline_run_id"].tolist())
        detail = query("pipeline_run", run_id=run_id)
        if detail:
            st.markdown("**Files**")
            st.dataframe(_frame(detail["files"]), hide_index=True, width="stretch")
            if detail["rejection_reasons"]:
                st.markdown("**Rejections in this run**")
                st.dataframe(_frame(detail["rejection_reasons"]), hide_index=True, width="stretch")

    st.subheader("Alerts")
    alerts = _frame(query("recent_alerts", limit=50))
    if alerts.empty:
        st.success("No alerts raised.")
    else:
        alerts["severity"] = alerts["severity"].map(lambda s: SEVERITY_LABELS.get(s, s))
        st.dataframe(
            alerts[["severity", "created_at", "alert_code", "message", "channels_delivered", "pipeline_run_id"]],
            hide_index=True,
            width="stretch",
        )


# Lineage

with lineage_tab:
    st.markdown(
        "Trace one claim through every stage: the source file and row it came from, "
        "staging, any rejections, and each version written to the fact table."
    )
    claim_id = st.text_input("Claim id", placeholder="e.g. CLM00016249").strip()

    if claim_id:
        trace = lineage(claim_id)

        if trace["fact"] is None and not trace["rejections"] and not trace["staging"]:
            st.warning(f"{claim_id} was never seen by the pipeline.")
        else:
            if trace["fact"]:
                fact = trace["fact"]
                cols = st.columns(4)
                cols[0].metric("Current status", fact["claim_status"])
                cols[1].metric("Billed", money(fact["billed_amount"]))
                cols[2].metric("Paid", money(fact["paid_amount"]))
                cols[3].metric("Source", f"{fact['source_filename']} · row {fact['source_row_number'] or '?'}")
            else:
                st.error("This claim never reached the fact table. See rejections below.")

            st.markdown("**Version history (fact_claims_history)**")
            st.dataframe(_frame(trace["history"]), hide_index=True, width="stretch")

            if trace["rejections"]:
                st.markdown("**Rejections**")
                for rejection in trace["rejections"]:
                    st.write(
                        f"`{rejection['rejection_reason']}` — {rejection['source_filename']} "
                        f"row {rejection['source_row_number']} (run {rejection['pipeline_run_id']})"
                    )
                    st.json(rejection["raw_record"], expanded=False)

            st.markdown("**Staging rows**")
            st.dataframe(_frame(trace["staging"]), hide_index=True, width="stretch")

            st.markdown("**Runs and source files**")
            st.dataframe(_frame(trace["runs"]), hide_index=True, width="stretch")
            st.dataframe(_frame(trace["source_files"]), hide_index=True, width="stretch")
