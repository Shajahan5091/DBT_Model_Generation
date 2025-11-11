import streamlit as st
import os
import base64
from style_utils import apply_style, render_header

apply_style()
render_header()

# -------------------- PAGE CONFIG --------------------
st.set_page_config(
    page_title="Snowflake DBT Automation Workbench",
    layout="wide",
    initial_sidebar_state="expanded"
)


# -------------------- HEADER: LOGO + TITLE --------------------
# Create a container for the header
header_container = st.container()

with header_container:
    col1, col2 = st.columns([1, 10])
    with col2:
        st.markdown(
            """
            <h1 style='margin-bottom:0; font-size:48px; font-weight:600; color:#1F4FE0;'>
                DBT Automation Workbench
            </h1>
            <p style='font-size:17px; color:#444;'>Accelerate dbt development with AI-powered model generation using Snowflake Cortex</p>
            """,
            unsafe_allow_html=True
        )

st.markdown("---")

# -------------------- MAIN CONTENT --------------------
st.markdown(
    """
    <p style='font-size:16px;'>
    Welcome to the <b>DBT Automation Workbench</b> — an intelligent workspace designed to automate dbt model and YAML generation using <b>Snowflake Cortex</b>.
    </p>
    <p style='font-size:15px;'>
    Use the sidebar to navigate between:
    </p>
    <ul style='font-size:15px;'>
        <li>🧱 <b>Model Generator</b> – Upload mapping documents and generate dbt models automatically.</li>
        <li>🗂 <b>File Explorer</b> – Browse and preview generated dbt files interactively.</li>
    </ul>
    """,
    unsafe_allow_html=True
)

st.info("👉 Select a page from the sidebar to get started.")
