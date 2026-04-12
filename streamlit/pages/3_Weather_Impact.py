"""Weather Impact — how weather conditions affect train reliability."""
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.bq_client import get_project_datasets, query_df

st.set_page_config(page_title="Weather Impact", layout="wide")
st.title("Weather Impact Analysis")
st.caption("Correlation between KNMI weather data and train reliability across corridors")

project, core_dataset, staging_dataset = get_project_datasets()

# ── Sidebar filters ──────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")
    start_date = st.date_input(
        "Start date",
        value=date.today() - timedelta(days=90),
        key="wx_start",
    )
    end_date = st.date_input(
        "End date",
        value=date.today() - timedelta(days=1),
        key="wx_end",
    )

# ── Data fetch ────────────────────────────────────────────────────────────
sql = f"""
select
    corridor_id, corridor_name, service_date,
    pct_on_time, avg_delay_min, total_departures,
    disruption_count, severe_delay_share,
    avg_temp_c, min_temp_c, max_temp_c,
    precipitation_mm, precipitation_duration_h,
    avg_wind_speed_kmh, max_wind_gust_kmh,
    is_stormy, is_heavy_rain,
    is_weekend, is_holiday, season
from `{project}.{core_dataset}.dm_multimodal_daily`
where service_date between '{start_date}' and '{end_date}'
  and avg_wind_speed_kmh is not null
order by service_date, corridor_name
"""
df = query_df(sql)

if df.empty:
    st.warning(
        "No weather data available for this date range. "
        "Ensure the KNMI ingestion DAG has run and dbt has been refreshed."
    )
    st.stop()

df["service_date"] = pd.to_datetime(df["service_date"])

# ── Weather condition summary ─────────────────────────────────────────────
st.subheader("Weather Conditions vs Reliability")

df["weather_condition"] = "Normal"
df.loc[df["precipitation_mm"] > 5, "weather_condition"] = "Rainy (>5mm)"
df.loc[df["avg_wind_speed_kmh"] > 40, "weather_condition"] = "Windy (>40 km/h)"
df.loc[df["is_heavy_rain"] == True, "weather_condition"] = "Heavy Rain (>10mm)"
df.loc[df["is_stormy"] == True, "weather_condition"] = "Storm (>75 km/h)"

condition_order = ["Normal", "Rainy (>5mm)", "Windy (>40 km/h)", "Heavy Rain (>10mm)", "Storm (>75 km/h)"]
weather_summary = (
    df.groupby("weather_condition")
    .agg(
        days=("service_date", "nunique"),
        avg_on_time=("pct_on_time", "mean"),
        avg_delay=("avg_delay_min", "mean"),
        avg_severe=("severe_delay_share", "mean"),
        avg_disruptions=("disruption_count", "mean"),
    )
    .reindex([c for c in condition_order if c in df["weather_condition"].unique()])
    .reset_index()
)

col1, col2 = st.columns([1, 1.5])

with col1:
    st.dataframe(
        weather_summary.rename(columns={
            "weather_condition": "Condition", "days": "Days",
            "avg_on_time": "Avg On-Time %", "avg_delay": "Avg Delay (min)",
            "avg_severe": "Severe Delay %", "avg_disruptions": "Avg Disruptions/Day",
        }).style.format({
            "Avg On-Time %": "{:.1f}", "Avg Delay (min)": "{:.2f}",
            "Severe Delay %": "{:.2f}", "Avg Disruptions/Day": "{:.1f}",
        }),
        use_container_width=True,
    )

with col2:
    fig_condition = px.bar(
        weather_summary,
        x="weather_condition", y="avg_on_time",
        color="avg_delay",
        color_continuous_scale="YlOrRd",
        labels={
            "weather_condition": "", "avg_on_time": "Avg On-Time %",
            "avg_delay": "Avg Delay (min)",
        },
    )
    fig_condition.update_layout(height=350, margin=dict(t=10))
    st.plotly_chart(fig_condition, use_container_width=True)

st.markdown("---")

# ── Wind impact ───────────────────────────────────────────────────────────
st.subheader("Wind Speed Impact")

col_w1, col_w2 = st.columns(2)

with col_w1:
    fig_wind = px.scatter(
        df, x="avg_wind_speed_kmh", y="pct_on_time",
        color="corridor_name", opacity=0.4,
        labels={
            "avg_wind_speed_kmh": "Avg Wind Speed (km/h)",
            "pct_on_time": "On-Time %", "corridor_name": "Corridor",
        },
    )
    fig_wind.update_layout(
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=10),
    )
    st.plotly_chart(fig_wind, use_container_width=True)

with col_w2:
    fig_gust = px.scatter(
        df, x="max_wind_gust_kmh", y="avg_delay_min",
        color="corridor_name", opacity=0.4,
        labels={
            "max_wind_gust_kmh": "Max Wind Gust (km/h)",
            "avg_delay_min": "Avg Delay (min)", "corridor_name": "Corridor",
        },
    )
    fig_gust.update_layout(
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=10),
    )
    st.plotly_chart(fig_gust, use_container_width=True)

# ── Wind speed bins ───────────────────────────────────────────────────────
df["wind_bin"] = pd.cut(
    df["avg_wind_speed_kmh"],
    bins=[0, 15, 30, 45, 60, 200],
    labels=["0-15", "15-30", "30-45", "45-60", "60+"],
)
wind_bins = (
    df.groupby("wind_bin", observed=True)
    .agg(days=("service_date", "nunique"), avg_on_time=("pct_on_time", "mean"), avg_delay=("avg_delay_min", "mean"))
    .reset_index()
)

fig_bins = go.Figure()
fig_bins.add_trace(
    go.Bar(x=wind_bins["wind_bin"], y=wind_bins["avg_on_time"], name="On-Time %", marker_color="#2ca02c")
)
fig_bins.add_trace(
    go.Bar(x=wind_bins["wind_bin"], y=wind_bins["avg_delay"], name="Avg Delay (min)", marker_color="#d62728")
)
fig_bins.update_layout(
    height=350, barmode="group",
    xaxis_title="Wind Speed Bin (km/h)", yaxis_title="Value",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(t=10),
)
st.plotly_chart(fig_bins, use_container_width=True)

st.markdown("---")

# ── Precipitation impact ──────────────────────────────────────────────────
st.subheader("Precipitation Impact")

col_p1, col_p2 = st.columns(2)

with col_p1:
    fig_rain = px.scatter(
        df, x="precipitation_mm", y="avg_delay_min",
        color="corridor_name", opacity=0.4,
        labels={
            "precipitation_mm": "Precipitation (mm)",
            "avg_delay_min": "Avg Delay (min)", "corridor_name": "Corridor",
        },
    )
    fig_rain.update_layout(
        height=380,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=10),
    )
    st.plotly_chart(fig_rain, use_container_width=True)

with col_p2:
    df["rain_bin"] = pd.cut(
        df["precipitation_mm"],
        bins=[-1, 0, 2, 5, 10, 100],
        labels=["Dry", "Light (0-2mm)", "Moderate (2-5mm)", "Heavy (5-10mm)", "Very heavy (10+mm)"],
    )
    rain_bins = (
        df.groupby("rain_bin", observed=True)
        .agg(days=("service_date", "nunique"), avg_on_time=("pct_on_time", "mean"))
        .reset_index()
    )
    fig_rain_bins = px.bar(
        rain_bins, x="rain_bin", y="avg_on_time",
        text="days",
        labels={"rain_bin": "", "avg_on_time": "Avg On-Time %", "days": "Days"},
        color="avg_on_time", color_continuous_scale="RdYlGn",
    )
    fig_rain_bins.update_layout(height=380, margin=dict(t=10))
    fig_rain_bins.update_traces(texttemplate="%{text} days", textposition="outside")
    st.plotly_chart(fig_rain_bins, use_container_width=True)

st.markdown("---")

# ── Temperature impact ────────────────────────────────────────────────────
st.subheader("Temperature Impact")

col_t1, col_t2 = st.columns(2)

with col_t1:
    fig_temp = px.scatter(
        df, x="avg_temp_c", y="pct_on_time",
        color="corridor_name", opacity=0.4,
        labels={
            "avg_temp_c": "Avg Temperature (C)",
            "pct_on_time": "On-Time %", "corridor_name": "Corridor",
        },
    )
    fig_temp.update_layout(
        height=380,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=10),
    )
    st.plotly_chart(fig_temp, use_container_width=True)

with col_t2:
    df["temp_bin"] = pd.cut(
        df["avg_temp_c"],
        bins=[-20, 0, 5, 10, 15, 20, 25, 40],
        labels=["<0", "0-5", "5-10", "10-15", "15-20", "20-25", "25+"],
    )
    temp_bins = (
        df.groupby("temp_bin", observed=True)
        .agg(days=("service_date", "nunique"), avg_on_time=("pct_on_time", "mean"))
        .reset_index()
    )
    fig_temp_bins = px.bar(
        temp_bins, x="temp_bin", y="avg_on_time",
        text="days",
        labels={"temp_bin": "Temperature (C)", "avg_on_time": "Avg On-Time %"},
        color="avg_on_time", color_continuous_scale="RdYlGn",
    )
    fig_temp_bins.update_layout(height=380, margin=dict(t=10))
    fig_temp_bins.update_traces(texttemplate="%{text} days", textposition="outside")
    st.plotly_chart(fig_temp_bins, use_container_width=True)

st.markdown("---")

# ── Seasonal patterns ─────────────────────────────────────────────────────
st.subheader("Seasonal Patterns")

seasonal = (
    df.groupby("season")
    .agg(
        avg_on_time=("pct_on_time", "mean"),
        avg_delay=("avg_delay_min", "mean"),
        avg_wind=("avg_wind_speed_kmh", "mean"),
        avg_rain=("precipitation_mm", "mean"),
        avg_temp=("avg_temp_c", "mean"),
    )
    .reindex(["winter", "spring", "summer", "autumn"])
    .reset_index()
)

fig_seasonal = go.Figure()
fig_seasonal.add_trace(go.Bar(
    x=seasonal["season"], y=seasonal["avg_on_time"],
    name="On-Time %", marker_color="#2ca02c",
))
fig_seasonal.add_trace(go.Scatter(
    x=seasonal["season"], y=seasonal["avg_wind"],
    name="Avg Wind (km/h)", yaxis="y2",
    line=dict(color="#1f77b4", width=3),
))
fig_seasonal.add_trace(go.Scatter(
    x=seasonal["season"], y=seasonal["avg_rain"],
    name="Avg Rain (mm)", yaxis="y2",
    line=dict(color="#ff7f0e", width=3, dash="dash"),
))
fig_seasonal.update_layout(
    height=380,
    yaxis=dict(title="On-Time %"),
    yaxis2=dict(title="Weather metric", overlaying="y", side="right"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(t=10),
)
st.plotly_chart(fig_seasonal, use_container_width=True)

# ── Corridor weather sensitivity ──────────────────────────────────────────
st.subheader("Corridor Weather Sensitivity")
st.caption("Which corridors are most affected by bad weather?")

sensitivity = (
    df.groupby(["corridor_name", df["avg_wind_speed_kmh"] > 30])
    .agg(avg_on_time=("pct_on_time", "mean"), days=("service_date", "nunique"))
    .reset_index()
)
sensitivity.columns = ["corridor_name", "is_windy", "avg_on_time", "days"]
sensitivity["condition"] = sensitivity["is_windy"].map({True: "Windy (>30 km/h)", False: "Calm (<30 km/h)"})

fig_sens = px.bar(
    sensitivity, x="corridor_name", y="avg_on_time",
    color="condition", barmode="group",
    labels={"corridor_name": "", "avg_on_time": "Avg On-Time %", "condition": ""},
    color_discrete_map={"Calm (<30 km/h)": "#2ca02c", "Windy (>30 km/h)": "#d62728"},
)
fig_sens.update_layout(
    height=380,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(t=10),
)
st.plotly_chart(fig_sens, use_container_width=True)
