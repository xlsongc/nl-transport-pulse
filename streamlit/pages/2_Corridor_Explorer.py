"""Corridor Explorer — deep-dive into a single corridor's performance."""
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.bq_client import get_project_datasets, query_df

st.set_page_config(page_title="Corridor Explorer", layout="wide")
st.title("Corridor Explorer")
st.caption("Station-level performance, operator mix, and disruption drivers")

project, core_dataset, staging_dataset = get_project_datasets()

# ── Sidebar filters ──────────────────────────────────────────────────────
sql_corridors = f"""
select distinct corridor_id, corridor_name
from `{project}.{core_dataset}.dm_multimodal_daily`
order by corridor_name
"""
df_corridors = query_df(sql_corridors)

if df_corridors.empty:
    st.warning("No corridor data available.")
    st.stop()

corridor_options = df_corridors.set_index("corridor_id")["corridor_name"].to_dict()

with st.sidebar:
    st.header("Filters")
    selected_corridor = st.selectbox(
        "Corridor",
        options=list(corridor_options.keys()),
        format_func=lambda x: corridor_options[x],
    )
    start_date = st.date_input(
        "Start date",
        value=date.today() - timedelta(days=90),
        key="explorer_start",
    )
    end_date = st.date_input(
        "End date",
        value=date.today() - timedelta(days=1),
        key="explorer_end",
    )
    volume_grain = st.selectbox(
        "Volume aggregation",
        options=["W", "M"],
        format_func=lambda x: "Weekly" if x == "W" else "Monthly",
    )

corridor_name = corridor_options[selected_corridor]

# ── Data fetch ────────────────────────────────────────────────────────────
sql_corridor = f"""
select *
from `{project}.{core_dataset}.dm_multimodal_daily`
where corridor_id = {selected_corridor}
  and service_date between '{start_date}' and '{end_date}'
order by service_date
"""
df_corridor = query_df(sql_corridor)

if df_corridor.empty:
    st.warning("No data for this corridor in the selected date range.")
    st.stop()

df_corridor["service_date"] = pd.to_datetime(df_corridor["service_date"])

# ── KPI scorecards ────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Departures", f"{int(df_corridor['total_departures'].sum()):,}")
with col2:
    st.metric("Avg On-Time %", f"{df_corridor['pct_on_time'].mean():.1f}%")
with col3:
    st.metric("Avg Delay", f"{df_corridor['avg_delay_min'].mean():.1f} min")
with col4:
    st.metric("Total Disruptions", f"{int(df_corridor['disruption_count'].sum()):,}")

st.caption(f"Corridor: **{corridor_name}** | {start_date} to {end_date}")
st.markdown("---")

# ── Volume and delay trend ────────────────────────────────────────────────
st.subheader("Volume and Delay Trend")

period_df = (
    df_corridor.groupby(pd.Grouper(key="service_date", freq=volume_grain))
    .agg(
        total_departures=("total_departures", "sum"),
        avg_delay_min=("avg_delay_min", "mean"),
        disruption_count=("disruption_count", "sum"),
        severe_delay_share=("severe_delay_share", "mean"),
    )
    .reset_index()
    .rename(columns={"service_date": "period_start"})
)

fig_volume = go.Figure()
fig_volume.add_trace(
    go.Bar(
        x=period_df["period_start"], y=period_df["total_departures"],
        name="Departures", marker_color="#1f77b4", opacity=0.7,
    )
)
fig_volume.add_trace(
    go.Scatter(
        x=period_df["period_start"], y=period_df["avg_delay_min"],
        name="Avg delay (min)", yaxis="y2",
        line=dict(color="#d62728", width=3),
    )
)
fig_volume.update_layout(
    height=380,
    yaxis=dict(title="Departures"),
    yaxis2=dict(title="Avg delay (min)", overlaying="y", side="right"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(t=10),
)
st.plotly_chart(fig_volume, use_container_width=True)

# ── Daily on-time trend ──────────────────────────────────────────────────
fig_ontime = px.area(
    df_corridor,
    x="service_date",
    y="pct_on_time",
    labels={"service_date": "", "pct_on_time": "On-Time %"},
)
fig_ontime.update_traces(line_color="#2ca02c", fillcolor="rgba(44,160,44,0.15)")
fig_ontime.update_layout(height=280, margin=dict(t=10))
st.plotly_chart(fig_ontime, use_container_width=True)

# ── Station-level performance ─────────────────────────────────────────────
st.subheader("Station Performance")

sql_stations = f"""
select
    f.station_code, s.station_name,
    avg(f.avg_delay_min) as avg_delay_min,
    avg(f.severe_delay_share) as avg_severe_delay_share,
    avg(f.pct_on_time) as avg_pct_on_time,
    sum(f.total_departures) as total_departures,
    sum(f.disruption_count) as total_disruptions
from `{project}.{core_dataset}.fct_train_performance` f
join `{project}.{core_dataset}.dim_stations` s
    on f.station_code = s.station_code
where f.corridor_id = {selected_corridor}
  and f.service_date between '{start_date}' and '{end_date}'
group by f.station_code, s.station_name
order by avg_delay_min desc
"""
df_stations = query_df(sql_stations)

if not df_stations.empty:
    col_s1, col_s2 = st.columns(2)
    with col_s1:
        fig_station_bar = px.bar(
            df_stations,
            x="station_name", y="avg_delay_min",
            color="avg_pct_on_time",
            color_continuous_scale="RdYlGn",
            labels={"station_name": "", "avg_delay_min": "Avg Delay (min)", "avg_pct_on_time": "On-Time %"},
        )
        fig_station_bar.update_layout(height=350, margin=dict(t=10))
        st.plotly_chart(fig_station_bar, use_container_width=True)

    with col_s2:
        fig_station_scatter = px.scatter(
            df_stations,
            x="total_departures", y="avg_delay_min",
            size="total_disruptions", hover_name="station_name",
            labels={
                "total_departures": "Total Departures",
                "avg_delay_min": "Avg Delay (min)",
                "total_disruptions": "Disruptions",
            },
        )
        fig_station_scatter.update_layout(height=350, margin=dict(t=10))
        st.plotly_chart(fig_station_scatter, use_container_width=True)

    st.dataframe(
        df_stations.rename(columns={
            "station_code": "Code", "station_name": "Station",
            "avg_delay_min": "Avg Delay (min)", "avg_severe_delay_share": "Severe Delay %",
            "avg_pct_on_time": "On-Time %", "total_departures": "Departures",
            "total_disruptions": "Disruptions",
        }).style.format({
            "Avg Delay (min)": "{:.2f}", "Severe Delay %": "{:.2f}", "On-Time %": "{:.1f}",
        }),
        use_container_width=True,
    )

# ── Operator mix ──────────────────────────────────────────────────────────
st.subheader("Operator Mix")

sql_operators = f"""
select
    coalesce(operator_name, 'Unknown') as operator_name,
    count(*) as departures,
    avg(delay_minutes) as avg_delay_min
from `{project}.{staging_dataset}.int_departures_combined` d
join `{project}.{core_dataset}.dim_stations` s
    on d.station_code = s.station_code
where s.corridor_id = {selected_corridor}
  and d.service_date between '{start_date}' and '{end_date}'
group by operator_name
having departures > 0
order by departures desc
"""
df_operators = query_df(sql_operators)

if not df_operators.empty:
    col_o1, col_o2 = st.columns([1, 1])
    with col_o1:
        fig_ops = px.pie(
            df_operators, values="departures", names="operator_name",
            hole=0.4,
        )
        fig_ops.update_layout(height=320, margin=dict(t=10, b=10))
        st.plotly_chart(fig_ops, use_container_width=True)

    with col_o2:
        st.dataframe(
            df_operators.rename(columns={
                "operator_name": "Operator", "departures": "Departures",
                "avg_delay_min": "Avg Delay (min)",
            }).style.format({"Avg Delay (min)": "{:.2f}"}),
            use_container_width=True,
        )

# ── Disruption causes ─────────────────────────────────────────────────────
st.subheader("Disruption Causes")

sql_causes = f"""
with historical as (
    select service_date,
        coalesce(cause_group, statistical_cause, cause, 'Unknown') as cause_family,
        station_code
    from `{project}.{staging_dataset}.stg_rdt_disruptions`,
    unnest(affected_station_codes) as station_code
    where service_date between '{start_date}' and '{end_date}'
),
live as (
    select service_date,
        coalesce(cause, disruption_type, 'Unknown') as cause_family,
        station_code
    from `{project}.{staging_dataset}.stg_ns_disruptions`,
    unnest(affected_station_codes) as station_code
    where service_date between '{start_date}' and '{end_date}'
),
combined as (
    select * from historical union all select * from live
)
select cause_family, count(*) as disruption_count
from combined c
join `{project}.{core_dataset}.dim_stations` s
    on c.station_code = s.station_code
where s.corridor_id = {selected_corridor}
group by cause_family
order by disruption_count desc
limit 12
"""
df_causes = query_df(sql_causes)

if not df_causes.empty:
    fig_causes = px.bar(
        df_causes, x="disruption_count", y="cause_family",
        orientation="h",
        labels={"disruption_count": "Disruptions", "cause_family": ""},
    )
    fig_causes.update_layout(
        height=380, yaxis=dict(categoryorder="total ascending"), margin=dict(t=10),
    )
    st.plotly_chart(fig_causes, use_container_width=True)

# ── Worst days ────────────────────────────────────────────────────────────
st.subheader("Worst Days")

worst = (
    df_corridor.sort_values(by=["avg_delay_min"], ascending=False)
    [["service_date", "avg_delay_min", "severe_delay_share", "disruption_count", "total_departures"]]
    .head(10)
    .rename(columns={
        "service_date": "Date", "avg_delay_min": "Avg Delay (min)",
        "severe_delay_share": "Severe Delay %", "disruption_count": "Disruptions",
        "total_departures": "Departures",
    })
)
st.dataframe(
    worst.style.format({"Avg Delay (min)": "{:.2f}", "Severe Delay %": "{:.2f}"}),
    use_container_width=True,
)
