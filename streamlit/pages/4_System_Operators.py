"""System & Operators — Dutch rail network structure, operator market share, and disruption history."""
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.bq_client import get_project_datasets, query_df

st.set_page_config(page_title="System & Operators", layout="wide")
st.title("System & Operators")
st.caption("Network structure, operator market share, delay attribution, and 15-year disruption trends")

project, core_dataset, staging_dataset = get_project_datasets()

# ── Default date range: full data coverage up to last complete month ──────
_last_month_end = date.today().replace(day=1) - timedelta(days=1)

# ── Sidebar filters ──────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")
    start_date = st.date_input(
        "Start date",
        value=date(2025, 3, 1),
        key="sys_start",
    )
    end_date = st.date_input(
        "End date",
        value=_last_month_end,
        key="sys_end",
    )

# ══════════════════════════════════════════════════════════════════════════
# Section 1: Network Scale KPIs
# ══════════════════════════════════════════════════════════════════════════

sql_kpis = f"""
select
    count(*) as total_departures,
    count(distinct station_code) as active_stations,
    count(distinct operator_name) as operators,
    count(distinct service_date) as service_days,
    round(avg(delay_minutes), 2) as avg_delay_min,
    round(countif(delay_minutes > 0) * 100.0 / count(*), 1) as pct_delayed
from `{project}.{staging_dataset}.int_departures_combined`
where service_date between '{start_date}' and '{end_date}'
"""
df_kpis = query_df(sql_kpis)

if df_kpis.empty or df_kpis["total_departures"].iloc[0] == 0:
    st.warning("No data available for this date range.")
    st.stop()

def _human_number(n: int) -> str:
    """Format large numbers: 21,587,839 → 21.6M"""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)

k = df_kpis.iloc[0]
col1, col2, col3, col4, col5, col6 = st.columns(6)
with col1:
    st.metric("Departures", _human_number(int(k['total_departures'])))
with col2:
    st.metric("Stations", f"{int(k['active_stations'])}")
with col3:
    st.metric("Operators", f"{int(k['operators'])}")
with col4:
    st.metric("Service Days", f"{int(k['service_days'])}")
with col5:
    st.metric("Avg Delay", f"{k['avg_delay_min']:.2f} min")
with col6:
    st.metric("% Delayed", f"{k['pct_delayed']:.1f}%")

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════
# Section 2: Monthly Departures by Operator
# ══════════════════════════════════════════════════════════════════════════
st.subheader("Monthly Departures by Operator")

sql_monthly_ops = f"""
select
    format_date('%Y-%m', service_date) as month,
    operator_name,
    count(*) as departures
from `{project}.{staging_dataset}.int_departures_combined`
where service_date between '{start_date}' and '{end_date}'
  and operator_name is not null
group by 1, 2
order by 1, 3 desc
"""
df_monthly_ops = query_df(sql_monthly_ops)

if not df_monthly_ops.empty:
    top_ops = (
        df_monthly_ops.groupby("operator_name")["departures"]
        .sum()
        .nlargest(8)
        .index.tolist()
    )
    df_monthly_ops["operator"] = df_monthly_ops["operator_name"].where(
        df_monthly_ops["operator_name"].isin(top_ops), "Other"
    )
    df_stacked = (
        df_monthly_ops.groupby(["month", "operator"])["departures"]
        .sum()
        .reset_index()
    )

    fig_monthly = px.bar(
        df_stacked,
        x="month", y="departures", color="operator",
        labels={"month": "", "departures": "Departures", "operator": "Operator"},
    )
    fig_monthly.update_layout(
        height=400, barmode="stack",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=10),
    )
    st.plotly_chart(fig_monthly, use_container_width=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════
# Section 2.5: Operator Profiles
# ══════════════════════════════════════════════════════════════════════════
st.subheader("Operator Profiles")

sql_profiles = f"""
select
    o.operator_name,
    o.full_name,
    o.country,
    o.operator_type,
    o.description,
    d.departures,
    d.avg_delay_min,
    d.pct_on_time
from `{project}.{core_dataset}.dim_operators` o
left join (
    select
        operator_name,
        count(*) as departures,
        round(avg(delay_minutes), 2) as avg_delay_min,
        round(countif(delay_minutes <= 0) * 100.0 / count(*), 1) as pct_on_time
    from `{project}.{staging_dataset}.int_departures_combined`
    where service_date between '{start_date}' and '{end_date}'
    group by 1
) d on o.operator_name = d.operator_name
where d.departures is not null
order by d.departures desc
"""
df_profiles = query_df(sql_profiles)

if not df_profiles.empty:
    type_colors = {
        "national": "#1f77b4",
        "regional": "#2ca02c",
        "international": "#ff7f0e",
        "cross-border": "#9467bd",
        "charter": "#8c564b",
    }

    for _, row in df_profiles.iterrows():
        color = type_colors.get(row["operator_type"], "#666")
        dep_str = _human_number(int(row["departures"])) if pd.notna(row["departures"]) else "—"
        delay_str = f"{row['avg_delay_min']:.2f}" if pd.notna(row["avg_delay_min"]) else "—"
        ontime_str = f"{row['pct_on_time']:.1f}%" if pd.notna(row["pct_on_time"]) else "—"

        st.markdown(
            f"**{row['full_name']}** (`{row['operator_name']}`) "
            f"&nbsp; :{color}[{row['operator_type'].upper()}] "
            f"&nbsp; {row['country']} "
            f"&nbsp;|&nbsp; {dep_str} deps &nbsp;|&nbsp; On-time: {ontime_str} &nbsp;|&nbsp; Delay: {delay_str} min  \n"
            f"_{row['description']}_"
        )

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════
# Section 3: Operator Reliability Ranking
# ══════════════════════════════════════════════════════════════════════════
st.subheader("Operator Reliability Ranking")

sql_ops = f"""
select
    operator_name,
    count(*) as departures,
    round(avg(delay_minutes), 2) as avg_delay_min,
    round(countif(delay_minutes <= 0) * 100.0 / count(*), 1) as pct_on_time,
    round(countif(delay_minutes > 5) * 100.0 / count(*), 1) as pct_over_5min,
    round(countif(delay_minutes > 15) * 100.0 / count(*), 1) as pct_over_15min
from `{project}.{staging_dataset}.int_departures_combined`
where service_date between '{start_date}' and '{end_date}'
  and operator_name is not null
group by 1
having departures >= 1000
order by departures desc
"""
df_ops = query_df(sql_ops)

if not df_ops.empty:
    col_o1, col_o2 = st.columns([1.2, 1])

    with col_o1:
        fig_ops = px.scatter(
            df_ops,
            x="avg_delay_min", y="pct_on_time",
            size="departures", hover_name="operator_name",
            color="operator_name",
            labels={
                "avg_delay_min": "Avg Delay (min)",
                "pct_on_time": "On-Time %",
                "departures": "Departures",
                "operator_name": "Operator",
            },
        )
        fig_ops.update_layout(
            height=400, showlegend=False,
            margin=dict(t=10),
        )
        st.plotly_chart(fig_ops, use_container_width=True)

    with col_o2:
        st.dataframe(
            df_ops.rename(columns={
                "operator_name": "Operator",
                "departures": "Departures",
                "avg_delay_min": "Avg Delay (min)",
                "pct_on_time": "On-Time %",
                "pct_over_5min": ">5min %",
                "pct_over_15min": ">15min %",
            }).style.format({
                "Departures": "{:,.0f}",
                "Avg Delay (min)": "{:.2f}",
                "On-Time %": "{:.1f}",
                ">5min %": "{:.2f}",
                ">15min %": "{:.3f}",
            }),
            use_container_width=True,
        )

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════
# Section 4: Busiest Stations
# ══════════════════════════════════════════════════════════════════════════
st.subheader("Busiest Stations (Top 25)")

sql_stations = f"""
with station_names as (
    select station_code, any_value(station_name) as station_name
    from `{project}.{staging_dataset}.stg_rdt_services`
    where station_name is not null
    group by 1
)
select
    d.station_code,
    coalesce(sn.station_name, d.station_code) as station_name,
    count(*) as departures,
    count(distinct d.operator_name) as operators,
    round(avg(d.delay_minutes), 2) as avg_delay_min,
    round(countif(d.delay_minutes <= 0) * 100.0 / count(*), 1) as pct_on_time
from `{project}.{staging_dataset}.int_departures_combined` d
left join station_names sn
    on d.station_code = sn.station_code
where d.service_date between '{start_date}' and '{end_date}'
group by 1, 2
order by departures desc
limit 25
"""
df_stations = query_df(sql_stations)

if not df_stations.empty:
    fig_stations = px.bar(
        df_stations,
        x="station_name", y="departures",
        color="avg_delay_min",
        color_continuous_scale="YlOrRd",
        labels={
            "station_name": "",
            "departures": "Departures",
            "avg_delay_min": "Avg Delay (min)",
        },
    )
    fig_stations.update_layout(height=400, margin=dict(t=10))
    st.plotly_chart(fig_stations, use_container_width=True)

    st.dataframe(
        df_stations.rename(columns={
            "station_code": "Code", "station_name": "Station",
            "departures": "Departures", "operators": "Operators",
            "avg_delay_min": "Avg Delay (min)", "pct_on_time": "On-Time %",
        }).style.format({
            "Departures": "{:,.0f}",
            "Avg Delay (min)": "{:.2f}",
            "On-Time %": "{:.1f}",
        }),
        use_container_width=True,
    )

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════
# Section 5: Network Scale Over Time (with station selector)
# ══════════════════════════════════════════════════════════════════════════
st.subheader("Network Scale Over Time")

# Station selector — use RDT station_name for full coverage
sql_all_stations = f"""
select
    station_code,
    any_value(station_name) as station_name
from `{project}.{staging_dataset}.stg_rdt_services`
where station_name is not null and station_name != ''
group by 1
order by station_name
"""
df_all_stations = query_df(sql_all_stations)

station_lookup = dict(zip(df_all_stations["station_name"], df_all_stations["station_code"]))
station_options = sorted(station_lookup.keys())
selected_stations = st.multiselect(
    "Filter by station (leave empty for all)",
    options=station_options,
    default=[],
    key="scale_stations",
)

# Build station filter clause
if not selected_stations:
    station_filter = ""
else:
    codes = [station_lookup[s] for s in selected_stations]
    codes_str = ",".join(f"'{c}'" for c in codes)
    station_filter = f"and station_code in ({codes_str})"

sql_scale = f"""
select
    format_date('%Y-%m', service_date) as month,
    count(distinct station_code) as active_stations,
    count(distinct operator_name) as active_operators,
    count(*) as departures,
    round(avg(delay_minutes), 2) as avg_delay_min
from `{project}.{staging_dataset}.int_departures_combined`
where service_date between '{start_date}' and '{end_date}'
  {station_filter}
group by 1
order by 1
"""
df_scale = query_df(sql_scale)

if not df_scale.empty:
    fig_scale = go.Figure()
    fig_scale.add_trace(go.Bar(
        x=df_scale["month"], y=df_scale["departures"],
        name="Departures", marker_color="#1f77b4", opacity=0.6,
    ))
    fig_scale.add_trace(go.Scatter(
        x=df_scale["month"], y=df_scale["active_stations"],
        name="Stations with Service", yaxis="y2",
        line=dict(color="#d62728", width=3),
    ))
    fig_scale.add_trace(go.Scatter(
        x=df_scale["month"], y=df_scale["avg_delay_min"],
        name="Avg Delay (min)", yaxis="y2",
        line=dict(color="#ff7f0e", width=2, dash="dash"),
    ))
    fig_scale.update_layout(
        height=380,
        yaxis=dict(title="Departures"),
        yaxis2=dict(title="Stations / Delay", overlaying="y", side="right"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=10),
    )
    st.plotly_chart(fig_scale, use_container_width=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════
# Section 6: Disruption Trends (15 years)
# ══════════════════════════════════════════════════════════════════════════
st.subheader("Disruption Trends (2011-2025)")
st.caption("Historical disruption data from rijdendetreinen.nl — 15 years of Dutch rail disruptions")

sql_disruptions_year = f"""
select
    extract(year from service_date) as year,
    count(*) as total_disruptions,
    round(avg(duration_minutes), 0) as avg_duration_min,
    countif(duration_minutes > 120) as long_disruptions
from `{project}.{staging_dataset}.stg_rdt_disruptions`
group by 1
order by 1
"""
df_dis_year = query_df(sql_disruptions_year)

if not df_dis_year.empty:
    fig_dis = go.Figure()
    fig_dis.add_trace(go.Bar(
        x=df_dis_year["year"], y=df_dis_year["total_disruptions"],
        name="Disruptions", marker_color="#d62728", opacity=0.7,
    ))
    fig_dis.add_trace(go.Scatter(
        x=df_dis_year["year"], y=df_dis_year["avg_duration_min"],
        name="Avg Duration (min)", yaxis="y2",
        line=dict(color="#1f77b4", width=3),
    ))
    fig_dis.update_layout(
        height=400,
        yaxis=dict(title="Disruptions"),
        yaxis2=dict(title="Avg Duration (min)", overlaying="y", side="right"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=10),
    )
    fig_dis.add_vrect(x0=2019.5, x1=2021.5, fillcolor="gray", opacity=0.15, line_width=0,
                      annotation_text="COVID", annotation_position="top left")
    st.plotly_chart(fig_dis, use_container_width=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════
# Section 7: Disruption Causes
# ══════════════════════════════════════════════════════════════════════════
st.subheader("Disruption Causes")

sql_causes = f"""
select
    extract(year from service_date) as year,
    cause_group,
    count(*) as disruptions
from `{project}.{staging_dataset}.stg_rdt_disruptions`
where cause_group is not null and cause_group != ''
group by 1, 2
order by 1, 3 desc
"""
df_causes = query_df(sql_causes)

if not df_causes.empty:
    col_c1, col_c2 = st.columns([1.5, 1])

    with col_c1:
        fig_causes_trend = px.area(
            df_causes,
            x="year", y="disruptions", color="cause_group",
            labels={"year": "", "disruptions": "Disruptions", "cause_group": "Cause"},
        )
        fig_causes_trend.update_layout(
            height=400,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(t=10),
        )
        st.plotly_chart(fig_causes_trend, use_container_width=True)

    with col_c2:
        cause_totals = df_causes.groupby("cause_group")["disruptions"].sum().reset_index()
        fig_pie = px.pie(
            cause_totals, values="disruptions", names="cause_group", hole=0.4,
        )
        fig_pie.update_layout(height=400, margin=dict(t=10, b=10))
        st.plotly_chart(fig_pie, use_container_width=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════
# Section 8: Delay Attribution by Operator
# ══════════════════════════════════════════════════════════════════════════
st.subheader("Delay Attribution by Operator")
st.caption("Which operators contribute most to total delay minutes across the network?")

sql_delay_attr = f"""
select
    operator_name,
    count(*) as departures,
    sum(greatest(delay_minutes, 0)) as total_delay_min,
    round(avg(case when delay_minutes > 0 then delay_minutes end), 2) as avg_delay_when_late
from `{project}.{staging_dataset}.int_departures_combined`
where service_date between '{start_date}' and '{end_date}'
  and operator_name is not null
group by 1
having departures >= 1000
order by total_delay_min desc
"""
df_delay_attr = query_df(sql_delay_attr)

if not df_delay_attr.empty:
    col_d1, col_d2 = st.columns(2)

    with col_d1:
        fig_delay_pie = px.pie(
            df_delay_attr.head(10),
            values="total_delay_min", names="operator_name", hole=0.4,
            labels={"total_delay_min": "Total Delay (min)", "operator_name": "Operator"},
        )
        fig_delay_pie.update_layout(
            height=380, title_text="Share of Total Delay Minutes",
            margin=dict(t=40, b=10),
        )
        st.plotly_chart(fig_delay_pie, use_container_width=True)

    with col_d2:
        fig_delay_bar = px.bar(
            df_delay_attr.head(10),
            x="operator_name", y="avg_delay_when_late",
            color="departures",
            color_continuous_scale="Blues",
            labels={
                "operator_name": "",
                "avg_delay_when_late": "Avg Delay When Late (min)",
                "departures": "Departures",
            },
        )
        fig_delay_bar.update_layout(height=380, margin=dict(t=10))
        st.plotly_chart(fig_delay_bar, use_container_width=True)
