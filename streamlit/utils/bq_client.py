"""BigQuery query helper for the Streamlit dashboard."""
import os

import pandas as pd
import streamlit as st
from google.cloud import bigquery


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    """Return a cached BigQuery client."""
    return bigquery.Client(project=os.environ.get("GCP_PROJECT_ID"))


def get_project_dataset() -> tuple[str, str]:
    """Return (project_id, core_dataset) from env vars."""
    project = os.environ.get("GCP_PROJECT_ID", "")
    dataset = os.environ.get("BQ_CORE_DATASET", "core_nl_transport")
    return project, dataset


def get_project_datasets() -> tuple[str, str, str]:
    """Return (project_id, core_dataset, staging_dataset) from env vars."""
    project = os.environ.get("GCP_PROJECT_ID", "")
    core_dataset = os.environ.get("BQ_CORE_DATASET", "core_nl_transport")
    staging_dataset = os.environ.get("BQ_STAGING_DATASET", "staging_nl_transport")
    return project, core_dataset, staging_dataset


@st.cache_data(ttl=300)
def query_df(sql: str) -> pd.DataFrame:
    """Execute a SQL query and return a pandas DataFrame. Cached for 5 minutes."""
    client = get_bq_client()
    return client.query(sql).to_dataframe()
