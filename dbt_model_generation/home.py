import streamlit as st
import os
import base64


st.set_page_config(
    page_title="Snowflake DBT Automation Workbench",
    layout="wide",
    initial_sidebar_state="expanded"
)
# Resolve image path


image_path = os.path.join(os.path.dirname(__file__), "assets", "elait.png")

# Read and encode the image
with open(image_path, "rb") as img:
    img_base64 = base64.b64encode(img.read()).decode()

# Display centered image
st.markdown(
    f"""
    <div style="text-align:center;">
        <img src="data:image/png;base64,{img_base64}" width="100">
    </div>
    """,
    unsafe_allow_html=True
)

st.title("⚙️ Snowflake DBT Automation Workbench")

st.markdown("""
Welcome to **DBT Automation Workbench**.  
Use the sidebar to navigate between:

- 🧱 **Model Generator** – upload mapping documents and generate dbt models automatically.  
- 🗂 **File Explorer** – browse and preview generated dbt files interactively.
""")

st.info("👉 Select a page from the sidebar to get started.")
