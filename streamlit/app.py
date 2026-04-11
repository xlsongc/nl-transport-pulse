import streamlit as st

st.set_page_config(
    page_title="NL Transport Pulse",
    layout="wide",
)

# Consistent color palette used across all pages
# Access via: from app import CORRIDOR_COLORS
CORRIDOR_COLORS = {
    "Amsterdam-Rotterdam": "#1f77b4",
    "Amsterdam-Utrecht": "#ff7f0e",
    "Den Haag-Rotterdam": "#2ca02c",
    "Utrecht-Eindhoven": "#d62728",
    "Utrecht-Arnhem": "#9467bd",
}

st.title("NL Transport Pulse")
st.caption("Dutch Rail Reliability & Weather Impact Dashboard")

st.markdown("---")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### Network Overview")
    st.markdown(
        "Corridor reliability scores, service volume trends, "
        "disruption cause analysis, and network stress map."
    )

with col2:
    st.markdown("### Corridor Explorer")
    st.markdown(
        "Deep-dive into a specific corridor — station-level "
        "performance, operator mix, and disruption drivers."
    )

with col3:
    st.markdown("### Weather Impact")
    st.markdown(
        "How wind, rain, and temperature affect train reliability. "
        "26 years of KNMI weather data correlated with rail performance."
    )

st.markdown("---")

st.markdown("##### Data Sources")
col_s1, col_s2, col_s3 = st.columns(3)
with col_s1:
    st.markdown("**NS / Rijdendetreinen.nl**  \nTrain departures, delays, disruptions")
with col_s2:
    st.markdown("**KNMI**  \nDaily weather (temperature, wind, precipitation)")
with col_s3:
    st.markdown("**Pipeline**  \nAirflow + dbt + BigQuery + Streamlit")

st.markdown("---")
st.caption(
    "Built by Xiaolong Song | "
    "DataTalksClub DE Zoomcamp 2026 | "
    "Architecture: NS API / KNMI / RDT Archive → GCS → BigQuery → dbt → Streamlit"
)
