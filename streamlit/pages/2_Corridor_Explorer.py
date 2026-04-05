"""Corridor Explorer — deep-dive into specific corridors and stations."""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import date, timedelta

from utils.bq_client import query_df, get_project_dataset

st.set_page_config(page_title="Corridor Explorer", layout="wide")
st.title("Corridor Explorer")

project, dataset = get_project_dataset()

# --- Corridor selector ---
sql_corridors = f"""
select distinct corridor_id, corridor_name
from `{project}.{dataset}.dm_multimodal_daily`
order by corridor_name
"""
df_corridors = query_df(sql_corridors)

if df_corridors.empty:
    st.warning("No corridor data available.")
    st.stop()

corridor_options = df_corridors.set_index("corridor_id")["corridor_name"].to_dict()

col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    selected_corridor = st.selectbox(
        "Select corridor",
        options=list(corridor_options.keys()),
        format_func=lambda x: corridor_options[x],
    )
with col2:
    start_date = st.date_input(
        "Start date",
        value=date.today() - timedelta(days=30),
        key="explorer_start",
    )
with col3:
    end_date = st.date_input(
        "End date",
        value=date.today() - timedelta(days=1),
        key="explorer_end",
    )

# --- Load corridor data ---
sql_corridor = f"""
select *
from `{project}.{dataset}.dm_multimodal_daily`
where corridor_id = {selected_corridor}
  and service_date between '{start_date}' and '{end_date}'
order by service_date
"""
df_corridor = query_df(sql_corridor)

if df_corridor.empty:
    st.warning("No data for this corridor in the selected date range.")
    st.stop()

# --- Summary metrics ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Avg On-Time %", f"{df_corridor['pct_on_time'].mean():.1f}%")
with col2:
    st.metric("Avg Delay (min)", f"{df_corridor['avg_delay_min'].mean():.1f}")
with col3:
    st.metric("Total Disruptions", int(df_corridor["disruption_count"].sum()))
with col4:
    avg_speed = df_corridor["avg_road_speed_kmh"].mean()
    speed_label = f"{avg_speed:.1f}" if avg_speed > 0 else "N/A"
    st.metric("Avg Road Speed (km/h)", speed_label)

# --- Dual-axis: train reliability + road speed ---
fig = make_subplots(specs=[[{"secondary_y": True}]])

fig.add_trace(
    go.Scatter(
        x=df_corridor["service_date"],
        y=df_corridor["pct_on_time"],
        name="Train On-Time %",
        line=dict(color="blue"),
    ),
    secondary_y=False,
)

fig.add_trace(
    go.Scatter(
        x=df_corridor["service_date"],
        y=df_corridor["avg_road_speed_kmh"],
        name="Road Avg Speed (km/h)",
        line=dict(color="orange", dash="dot"),
    ),
    secondary_y=True,
)

fig.update_layout(
    title="Train Reliability vs Road Traffic",
    height=400,
)
fig.update_yaxes(title_text="On-Time %", secondary_y=False)
fig.update_yaxes(title_text="Road Speed (km/h)", secondary_y=True)
st.plotly_chart(fig, use_container_width=True)

# --- Station-level breakdown ---
st.subheader("Station-Level Performance")

sql_stations = f"""
select
    f.station_code,
    s.station_name,
    avg(f.pct_on_time) as avg_pct_on_time,
    avg(f.avg_delay_min) as avg_delay_min,
    avg(f.severe_delay_share) as avg_severe_delay_share,
    sum(f.disruption_count) as total_disruptions
from `{project}.{dataset}.fct_train_performance` f
join `{project}.{dataset}.dim_stations` s
    on f.station_code = s.station_code
where f.corridor_id = {selected_corridor}
  and f.service_date between '{start_date}' and '{end_date}'
group by f.station_code, s.station_name
order by avg_pct_on_time asc
"""
df_stations = query_df(sql_stations)

if not df_stations.empty:
    fig_stations = px.bar(
        df_stations,
        x="station_name",
        y="avg_pct_on_time",
        color="avg_pct_on_time",
        color_continuous_scale=["red", "yellow", "green"],
        range_color=[50, 100],
        title="Average On-Time % by Station",
        labels={"avg_pct_on_time": "On-Time %", "station_name": "Station"},
    )
    fig_stations.update_layout(height=400)
    st.plotly_chart(fig_stations, use_container_width=True)

    st.dataframe(
        df_stations.rename(columns={
            "station_code": "Code",
            "station_name": "Station",
            "avg_pct_on_time": "Avg On-Time %",
            "avg_delay_min": "Avg Delay (min)",
            "avg_severe_delay_share": "Severe Delay %",
            "total_disruptions": "Disruptions",
        }).style.format({
            "Avg On-Time %": "{:.1f}",
            "Avg Delay (min)": "{:.1f}",
            "Severe Delay %": "{:.1f}",
        }),
        use_container_width=True,
    )

# --- Weekday vs Weekend comparison ---
st.subheader("Weekday vs Weekend")
df_corridor["day_type"] = df_corridor["is_weekend"].map({True: "Weekend", False: "Weekday"})

fig_daytype = px.box(
    df_corridor,
    x="day_type",
    y="pct_on_time",
    color="day_type",
    title="On-Time % Distribution: Weekday vs Weekend",
    labels={"pct_on_time": "On-Time %", "day_type": ""},
)
fig_daytype.update_layout(height=350, showlegend=False)
st.plotly_chart(fig_daytype, use_container_width=True)
