import streamlit as st

st.set_page_config(
    page_title="NL Transport Pulse",
    page_icon="🚆",
    layout="wide",
)

st.title("NL Transport Pulse")
st.markdown(
    "Dutch multimodal transport reliability dashboard. "
    "Use the sidebar to navigate between pages."
)
st.markdown("---")
st.markdown(
    "**Pages:**\n"
    "- **Network Overview** — Corridor reliability scores, map, and trends\n"
    "- **Corridor Explorer** — Deep-dive into specific corridors and stations"
)
