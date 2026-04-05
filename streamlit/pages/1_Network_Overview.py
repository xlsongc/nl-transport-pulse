"""Network Overview — rail-first reliability, volume, and disruption patterns."""
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.bq_client import get_project_datasets, query_df

st.set_page_config(page_title="Network Overview", layout="wide")
st.title("Network Overview")

project, core_dataset, staging_dataset = get_project_datasets()


def _format_period_label(value: str) -> str:
    if value == "W":
        return "Weekly"
    return "Monthly"


def _prepare_period_aggregation(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    grouped = (
        df.assign(service_date=pd.to_datetime(df["service_date"]))
        .groupby(
            [
                pd.Grouper(key="service_date", freq=freq),
                "corridor_name",
            ],
            as_index=False,
        )
        .agg(
            total_departures=("total_departures", "sum"),
            disruption_count=("disruption_count", "sum"),
            avg_delay_min=("avg_delay_min", "mean"),
            severe_delay_share=("severe_delay_share", "mean"),
        )
        .rename(columns={"service_date": "period_start"})
    )
    return grouped


col1, col2, col3 = st.columns([1.2, 1.2, 1])
with col1:
    start_date = st.date_input(
        "Start date",
        value=date.today() - timedelta(days=90),
        key="overview_start",
    )
with col2:
    end_date = st.date_input(
        "End date",
        value=date.today() - timedelta(days=1),
        key="overview_end",
    )
with col3:
    aggregation = st.selectbox(
        "Volume aggregation",
        options=["W", "M"],
        format_func=_format_period_label,
    )

sql_multimodal = f"""
select
    corridor_id,
    corridor_name,
    service_date,
    pct_on_time,
    avg_delay_min,
    total_departures,
    disruption_count,
    severe_delay_share,
    avg_road_speed_kmh,
    total_congestion_minutes
from `{project}.{core_dataset}.dm_multimodal_daily`
where service_date between '{start_date}' and '{end_date}'
order by service_date, corridor_name
"""
df = query_df(sql_multimodal)

if df.empty:
    st.warning("No data available for the selected date range.")
    st.stop()

df["service_date"] = pd.to_datetime(df["service_date"])
latest_date = df["service_date"].max()
latest = df[df["service_date"] == latest_date].copy()
period_df = _prepare_period_aggregation(df, aggregation)

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total departures", f"{int(df['total_departures'].sum()):,}")
with col2:
    st.metric("Avg delay (min)", f"{df['avg_delay_min'].mean():.1f}")
with col3:
    st.metric("Severe delay share", f"{df['severe_delay_share'].mean():.1f}%")
with col4:
    st.metric("Total disruptions", f"{int(df['disruption_count'].sum()):,}")

st.caption(f"Latest service date in view: {latest_date.date()}")

st.subheader("Service Volume and Disruptions")
fig_volume = px.bar(
    period_df,
    x="period_start",
    y="total_departures",
    color="corridor_name",
    title=f"{_format_period_label(aggregation)} departures by corridor",
    labels={"period_start": "Period", "total_departures": "Departures"},
)
fig_volume.update_layout(height=380)
st.plotly_chart(fig_volume, use_container_width=True)

st.subheader("Network Stress Map")
station_sql = f"""
select distinct
    station_code,
    station_name,
    corridor_id,
    corridor_name,
    lat,
    lon
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
    zoom=6,
    center={"lat": 52.1, "lon": 5.1},
    mapbox_style="carto-positron",
    title="Latest-day station stress by corridor",
)
fig_map.update_layout(height=500, margin=dict(l=0, r=0, t=30, b=0))
st.plotly_chart(fig_map, use_container_width=True)

st.subheader("Reliability Calendar")
calendar_metric_label = st.selectbox(
    "Calendar metric",
    options=[
        "Average delay (min)",
        "Severe delay share (%)",
        "Disruptions",
        "On-time (%)",
    ],
)
calendar_metric_map = {
    "Average delay (min)": "avg_delay_min",
    "Severe delay share (%)": "severe_delay_share",
    "Disruptions": "disruption_count",
    "On-time (%)": "pct_on_time",
}
calendar_metric = calendar_metric_map[calendar_metric_label]
calendar_pivot = (
    df.pivot_table(
        index="corridor_name",
        columns="service_date",
        values=calendar_metric,
        aggfunc="mean",
    )
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
fig_calendar.update_layout(height=320, margin=dict(l=0, r=0, t=30, b=0))
st.plotly_chart(fig_calendar, use_container_width=True)

st.subheader("Disruption Cause Mix")
sql_causes = f"""
with historical as (
    select
        service_date,
        coalesce(cause_group, statistical_cause, cause, 'Unknown') as cause_family
    from `{project}.{staging_dataset}.stg_rdt_disruptions`
    where service_date between '{start_date}' and '{end_date}'
),
live as (
    select
        service_date,
        coalesce(cause, disruption_type, 'Unknown') as cause_family
    from `{project}.{staging_dataset}.stg_ns_disruptions`
    where service_date between '{start_date}' and '{end_date}'
),
combined as (
    select * from historical
    union all
    select * from live
)
select
    date_trunc(service_date, month) as period_start,
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
        title="Monthly disruption causes",
        labels={"period_start": "Month", "disruption_count": "Disruptions"},
    )
    fig_causes.update_layout(height=380)
    st.plotly_chart(fig_causes, use_container_width=True)

st.subheader("Worst Corridor-Days")
worst_days = (
    df.sort_values(
        by=["avg_delay_min", "severe_delay_share", "disruption_count"],
        ascending=[False, False, False],
    )[["service_date", "corridor_name", "avg_delay_min", "severe_delay_share", "disruption_count", "total_departures"]]
    .head(12)
    .rename(
        columns={
            "service_date": "Date",
            "corridor_name": "Corridor",
            "avg_delay_min": "Avg Delay (min)",
            "severe_delay_share": "Severe Delay %",
            "disruption_count": "Disruptions",
            "total_departures": "Departures",
        }
    )
)
st.dataframe(
    worst_days.style.format(
        {
            "Avg Delay (min)": "{:.2f}",
            "Severe Delay %": "{:.2f}",
        }
    ),
    use_container_width=True,
)
