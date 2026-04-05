"""Network Overview — corridor reliability scores, map, and daily trends."""
import streamlit as st
import plotly.express as px
from datetime import date, timedelta

from utils.bq_client import query_df, get_project_dataset

st.set_page_config(page_title="Network Overview", layout="wide")
st.title("Network Overview")

project, dataset = get_project_dataset()

# --- Date picker ---
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input(
        "Start date",
        value=date.today() - timedelta(days=14),
        key="overview_start",
    )
with col2:
    end_date = st.date_input(
        "End date",
        value=date.today() - timedelta(days=1),
        key="overview_end",
    )

# --- Load data ---
sql_multimodal = f"""
select
    corridor_id,
    corridor_name,
    service_date,
    pct_on_time,
    avg_delay_min,
    total_departures,
    disruption_count,
    avg_road_speed_kmh,
    total_congestion_minutes
from `{project}.{dataset}.dm_multimodal_daily`
where service_date between '{start_date}' and '{end_date}'
order by service_date
"""
df = query_df(sql_multimodal)

if df.empty:
    st.warning("No data available for the selected date range.")
    st.stop()

# --- Scorecards ---
latest_date = df["service_date"].max()
latest = df[df["service_date"] == latest_date]

col1, col2, col3, col4 = st.columns(4)
with col1:
    overall_on_time = latest["pct_on_time"].mean()
    st.metric("Overall On-Time %", f"{overall_on_time:.1f}%")
with col2:
    total_disruptions = latest["disruption_count"].sum()
    st.metric("Active Disruptions", int(total_disruptions))
with col3:
    worst = latest.loc[latest["pct_on_time"].idxmin()]
    st.metric("Worst Corridor", worst["corridor_name"])
with col4:
    avg_delay = latest["avg_delay_min"].mean()
    st.metric("Avg Delay (min)", f"{avg_delay:.1f}")

st.markdown(f"*Latest service date: {latest_date}*")

# --- Corridor reliability map ---
sql_stations = f"""
select distinct
    station_code,
    station_name,
    corridor_id,
    corridor_name,
    lat,
    lon
from `{project}.{dataset}.dim_stations`
"""
df_stations = query_df(sql_stations)

station_scores = df_stations.merge(
    latest[["corridor_id", "pct_on_time"]],
    on="corridor_id",
    how="left",
)

fig_map = px.scatter_mapbox(
    station_scores,
    lat="lat",
    lon="lon",
    color="pct_on_time",
    hover_name="station_name",
    hover_data=["corridor_name", "pct_on_time"],
    color_continuous_scale=["red", "yellow", "green"],
    range_color=[50, 100],
    zoom=6,
    center={"lat": 52.1, "lon": 5.1},
    mapbox_style="carto-positron",
    title="Station Reliability (latest service date)",
)
fig_map.update_layout(height=500, margin=dict(l=0, r=0, t=30, b=0))
st.plotly_chart(fig_map, use_container_width=True)

# --- Daily trend by corridor ---
fig_trend = px.line(
    df,
    x="service_date",
    y="pct_on_time",
    color="corridor_name",
    title="Daily On-Time % by Corridor",
    labels={"pct_on_time": "On-Time %", "service_date": "Date"},
)
fig_trend.update_layout(height=400)
st.plotly_chart(fig_trend, use_container_width=True)

# --- Congestion trend ---
fig_congestion = px.bar(
    df,
    x="service_date",
    y="total_congestion_minutes",
    color="corridor_name",
    title="Daily Road Congestion Minutes by Corridor",
    labels={"total_congestion_minutes": "Congestion (min)", "service_date": "Date"},
)
fig_congestion.update_layout(height=400)
st.plotly_chart(fig_congestion, use_container_width=True)
