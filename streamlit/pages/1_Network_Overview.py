"""Network Overview — corridor reliability, volume, disruptions, and weather impact."""
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.bq_client import get_project_datasets, query_df

st.set_page_config(page_title="Network Overview", layout="wide")
st.title("Network Overview")
st.caption("Cross-corridor reliability and service trends")

project, core_dataset, staging_dataset = get_project_datasets()

# ── Sidebar filters ──────────────────────────────────────────────────────
_last_month_end = date.today().replace(day=1) - timedelta(days=1)

with st.sidebar:
    st.header("Filters")
    start_date = st.date_input(
        "Start date",
        value=date(2025, 3, 1),
        key="overview_start",
    )
    end_date = st.date_input(
        "End date",
        value=_last_month_end,
        key="overview_end",
    )
    aggregation = st.selectbox(
        "Volume aggregation",
        options=["W", "M"],
        format_func=lambda x: "Weekly" if x == "W" else "Monthly",
    )

# ── Data fetch ────────────────────────────────────────────────────────────
sql = f"""
select
    corridor_id, corridor_name, service_date,
    pct_on_time, avg_delay_min, total_departures,
    disruption_count, severe_delay_share,
    avg_road_speed_kmh, total_congestion_minutes,
    avg_temp_c, precipitation_mm, avg_wind_speed_kmh,
    max_wind_gust_kmh, is_stormy, is_heavy_rain
from `{project}.{core_dataset}.dm_multimodal_daily`
where service_date between '{start_date}' and '{end_date}'
order by service_date, corridor_name
"""
df = query_df(sql)

if df.empty:
    st.warning("No data available for the selected date range.")
    st.stop()

df["service_date"] = pd.to_datetime(df["service_date"])
latest_date = df["service_date"].max()
latest = df[df["service_date"] == latest_date].copy()
has_weather = "avg_wind_speed_kmh" in df.columns and df["avg_wind_speed_kmh"].notna().any()

# ── KPI scorecards ────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Departures", f"{int(df['total_departures'].sum()):,}")
with col2:
    st.metric("Avg On-Time %", f"{df['pct_on_time'].mean():.1f}%")
with col3:
    st.metric("Avg Delay", f"{df['avg_delay_min'].mean():.1f} min")
with col4:
    st.metric("Total Disruptions", f"{int(df['disruption_count'].sum()):,}")

st.caption(f"Data range: {df['service_date'].min().date()} to {latest_date.date()}")
st.markdown("---")

# ── Reliability trend ─────────────────────────────────────────────────────
st.subheader("On-Time Performance")

fig_trend = px.line(
    df.sort_values(["service_date", "corridor_name"]),
    x="service_date",
    y="pct_on_time",
    color="corridor_name",
    labels={"service_date": "", "pct_on_time": "On-Time %", "corridor_name": "Corridor"},
)
fig_trend.update_layout(
    height=380,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(t=10),
)
st.plotly_chart(fig_trend, use_container_width=True)

# ── Service volume ────────────────────────────────────────────────────────
st.subheader("Service Volume")

period_df = (
    df.assign(service_date=pd.to_datetime(df["service_date"]))
    .groupby([pd.Grouper(key="service_date", freq=aggregation), "corridor_name"])
    .agg(
        total_departures=("total_departures", "sum"),
        disruption_count=("disruption_count", "sum"),
    )
    .reset_index()
    .rename(columns={"service_date": "period_start"})
)

fig_volume = px.bar(
    period_df,
    x="period_start",
    y="total_departures",
    color="corridor_name",
    labels={"period_start": "", "total_departures": "Departures", "corridor_name": "Corridor"},
)
fig_volume.update_layout(
    height=350,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(t=10),
)
st.plotly_chart(fig_volume, use_container_width=True)

# ── Network stress map ────────────────────────────────────────────────────
st.subheader("Network Stress Map")
st.caption("Latest day — bubble size = severe delay share, color = avg delay")

station_sql = f"""
select distinct station_code, station_name, corridor_id, corridor_name, lat, lon
from `{project}.{core_dataset}.dim_stations`
"""
df_stations = query_df(station_sql)
station_scores = df_stations.merge(
    latest[["corridor_id", "avg_delay_min", "severe_delay_share"]],
    on="corridor_id",
    how="left",
)
fig_map = px.scatter_mapbox(
    station_scores,
    lat="lat",
    lon="lon",
    color="avg_delay_min",
    size="severe_delay_share",
    hover_name="station_name",
    hover_data=["corridor_name", "avg_delay_min", "severe_delay_share"],
    color_continuous_scale="YlOrRd",
    zoom=6.5,
    center={"lat": 52.1, "lon": 5.1},
    mapbox_style="carto-positron",
)
fig_map.update_layout(height=480, margin=dict(l=0, r=0, t=0, b=0))
st.plotly_chart(fig_map, use_container_width=True)

# ── Reliability calendar ──────────────────────────────────────────────────
st.subheader("Reliability Calendar")

calendar_metric_label = st.selectbox(
    "Metric",
    options=["On-time (%)", "Average delay (min)", "Severe delay share (%)", "Disruptions"],
    key="cal_metric",
)
calendar_metric_map = {
    "Average delay (min)": "avg_delay_min",
    "Severe delay share (%)": "severe_delay_share",
    "Disruptions": "disruption_count",
    "On-time (%)": "pct_on_time",
}
calendar_metric = calendar_metric_map[calendar_metric_label]
calendar_pivot = (
    df.pivot_table(index="corridor_name", columns="service_date", values=calendar_metric, aggfunc="mean")
    .sort_index()
)
fig_calendar = go.Figure(
    data=go.Heatmap(
        z=calendar_pivot.values,
        x=[d.date() for d in calendar_pivot.columns],
        y=calendar_pivot.index.tolist(),
        colorscale="YlOrRd" if calendar_metric != "pct_on_time" else "RdYlGn",
        reversescale=(calendar_metric == "pct_on_time"),
        hovertemplate="Corridor: %{y}<br>Date: %{x}<br>Value: %{z:.2f}<extra></extra>",
    )
)
fig_calendar.update_layout(height=280, margin=dict(l=0, r=0, t=0, b=0))
st.plotly_chart(fig_calendar, use_container_width=True)

# ── Disruption causes ─────────────────────────────────────────────────────
st.subheader("Disruption Causes")

cause_grain = st.selectbox("Aggregation", options=["Week", "Month"], key="cause_grain")
cause_trunc = "week" if cause_grain == "Week" else "month"

sql_causes = f"""
with historical as (
    select service_date,
        coalesce(cause_group, statistical_cause, cause, 'Unknown') as cause_family
    from `{project}.{staging_dataset}.stg_rdt_disruptions`
    where service_date between '{start_date}' and '{end_date}'
),
live as (
    select service_date,
        coalesce(cause, disruption_type, 'Unknown') as cause_family
    from `{project}.{staging_dataset}.stg_ns_disruptions`
    where service_date between '{start_date}' and '{end_date}'
),
combined as (
    select * from historical union all select * from live
)
select
    date_trunc(service_date, {cause_trunc}) as period_start,
    cause_family,
    count(*) as disruption_count
from combined
group by period_start, cause_family
order by period_start, disruption_count desc
"""
df_causes = query_df(sql_causes)

if not df_causes.empty:
    fig_causes = px.bar(
        df_causes,
        x="period_start",
        y="disruption_count",
        color="cause_family",
        labels={"period_start": "", "disruption_count": "Disruptions", "cause_family": "Cause"},
    )
    fig_causes.update_layout(
        height=380,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=10),
    )
    st.plotly_chart(fig_causes, use_container_width=True)

# ── Worst days ────────────────────────────────────────────────────────────
st.subheader("Worst Corridor-Days")

worst_cols = ["service_date", "corridor_name", "avg_delay_min", "severe_delay_share", "disruption_count", "total_departures"]
worst_rename = {
    "service_date": "Date", "corridor_name": "Corridor",
    "avg_delay_min": "Avg Delay (min)", "severe_delay_share": "Severe Delay %",
    "disruption_count": "Disruptions", "total_departures": "Departures",
}
fmt = {"Avg Delay (min)": "{:.2f}", "Severe Delay %": "{:.2f}"}
if has_weather:
    worst_cols.extend(["avg_wind_speed_kmh", "precipitation_mm"])
    worst_rename["avg_wind_speed_kmh"] = "Wind (km/h)"
    worst_rename["precipitation_mm"] = "Rain (mm)"
    fmt["Wind (km/h)"] = "{:.1f}"
    fmt["Rain (mm)"] = "{:.1f}"

worst_days = (
    df.sort_values(by=["avg_delay_min"], ascending=False)[worst_cols]
    .head(12)
    .rename(columns=worst_rename)
)
st.dataframe(worst_days.style.format(fmt), use_container_width=True)
