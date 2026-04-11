"""Corridor Explorer — corridor-level rail history, operators, and disruption drivers."""
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.bq_client import get_project_datasets, query_df

st.set_page_config(page_title="Corridor Explorer", layout="wide")
st.title("Corridor Explorer")

project, core_dataset, staging_dataset = get_project_datasets()

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

col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
with col1:
    selected_corridor = st.selectbox(
        "Select corridor",
        options=list(corridor_options.keys()),
        format_func=lambda x: corridor_options[x],
    )
with col2:
    start_date = st.date_input(
        "Start date",
        value=date.today() - timedelta(days=90),
        key="explorer_start",
    )
with col3:
    end_date = st.date_input(
        "End date",
        value=date.today() - timedelta(days=1),
        key="explorer_end",
    )
with col4:
    volume_grain = st.selectbox(
        "Volume aggregation",
        options=["W", "M"],
        format_func=lambda x: "Weekly" if x == "W" else "Monthly",
    )

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

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total departures", f"{int(df_corridor['total_departures'].sum()):,}")
with col2:
    st.metric("Avg delay (min)", f"{df_corridor['avg_delay_min'].mean():.1f}")
with col3:
    st.metric("Severe delay share", f"{df_corridor['severe_delay_share'].mean():.1f}%")
with col4:
    st.metric("Total disruptions", f"{int(df_corridor['disruption_count'].sum()):,}")

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
        x=period_df["period_start"],
        y=period_df["total_departures"],
        name="Departures",
        marker_color="#1f77b4",
    )
)
fig_volume.add_trace(
    go.Scatter(
        x=period_df["period_start"],
        y=period_df["avg_delay_min"],
        name="Avg delay (min)",
        yaxis="y2",
        line=dict(color="#d62728", width=3),
    )
)
fig_volume.update_layout(
    title="Service volume with average delay overlay",
    height=400,
    yaxis=dict(title="Departures"),
    yaxis2=dict(title="Avg delay (min)", overlaying="y", side="right"),
)
st.plotly_chart(fig_volume, use_container_width=True)

st.subheader("Worst Days")
worst_days = (
    df_corridor.sort_values(
        by=["avg_delay_min", "severe_delay_share", "disruption_count"],
        ascending=[False, False, False],
    )[["service_date", "avg_delay_min", "severe_delay_share", "disruption_count", "total_departures"]]
    .head(10)
    .rename(
        columns={
            "service_date": "Date",
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
    fig_ops = px.treemap(
        df_operators,
        path=["operator_name"],
        values="departures",
        color="avg_delay_min",
        color_continuous_scale="YlOrRd",
        title="Operator mix by departure volume",
    )
    fig_ops.update_layout(height=420, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig_ops, use_container_width=True)

    st.dataframe(
        df_operators.rename(
            columns={
                "operator_name": "Operator",
                "departures": "Departures",
                "avg_delay_min": "Avg Delay (min)",
            }
        ).style.format({"Avg Delay (min)": "{:.2f}"}),
        use_container_width=True,
    )

st.subheader("Disruption Causes in Corridor")
sql_causes = f"""
with historical as (
    select
        service_date,
        coalesce(cause_group, statistical_cause, cause, 'Unknown') as cause_family,
        station_code
    from `{project}.{staging_dataset}.stg_rdt_disruptions`,
    unnest(affected_station_codes) as station_code
    where service_date between '{start_date}' and '{end_date}'
),
live as (
    select
        service_date,
        coalesce(cause, disruption_type, 'Unknown') as cause_family,
        station_code
    from `{project}.{staging_dataset}.stg_ns_disruptions`,
    unnest(affected_station_codes) as station_code
    where service_date between '{start_date}' and '{end_date}'
),
combined as (
    select * from historical
    union all
    select * from live
)
select
    cause_family,
    count(*) as disruption_count
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
        df_causes,
        x="disruption_count",
        y="cause_family",
        orientation="h",
        title="Top disruption causes in selected corridor",
        labels={"disruption_count": "Disruptions", "cause_family": "Cause"},
    )
    fig_causes.update_layout(height=420, yaxis=dict(categoryorder="total ascending"))
    st.plotly_chart(fig_causes, use_container_width=True)

st.subheader("Station-Level Performance")
sql_stations = f"""
select
    f.station_code,
    s.station_name,
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
order by avg_delay_min desc, avg_severe_delay_share desc
"""
df_stations = query_df(sql_stations)

if not df_stations.empty:
    fig_stations = px.bar(
        df_stations,
        x="station_name",
        y="avg_delay_min",
        color="avg_severe_delay_share",
        color_continuous_scale="YlOrRd",
        title="Average station delay with severe-delay intensity",
        labels={
            "station_name": "Station",
            "avg_delay_min": "Avg Delay (min)",
            "avg_severe_delay_share": "Severe Delay %",
        },
    )
    fig_stations.update_layout(height=400)
    st.plotly_chart(fig_stations, use_container_width=True)

    st.dataframe(
        df_stations.rename(
            columns={
                "station_code": "Code",
                "station_name": "Station",
                "avg_delay_min": "Avg Delay (min)",
                "avg_severe_delay_share": "Severe Delay %",
                "avg_pct_on_time": "Avg On-Time %",
                "total_departures": "Departures",
                "total_disruptions": "Disruptions",
            }
        ).style.format(
            {
                "Avg Delay (min)": "{:.2f}",
                "Severe Delay %": "{:.2f}",
                "Avg On-Time %": "{:.2f}",
            }
        ),
        use_container_width=True,
    )
