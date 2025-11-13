import streamlit as st
import os
from style_utils import apply_style, render_header

apply_style()
render_header()

st.set_page_config(page_title="dbt File Viewer", layout="wide")
st.title("📂 File Explorer")

# Base directory (change if needed)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# --- Session state setup ---
if "selected_folder" not in st.session_state:
    st.session_state.selected_folder = BASE_DIR
if "selected_file" not in st.session_state:
    st.session_state.selected_file = None

# --- Helper function ---
def list_directory(path):
    folders, files = [], []
    for item in os.listdir(path):
        full = os.path.join(path, item)
        if os.path.isdir(full):
            folders.append(item)
        else:
            files.append(item)
    return sorted(folders), sorted(files)

# --- Sidebar Navigation ---
st.sidebar.markdown("### 📂 Folder Navigation")

current_folder = st.session_state.selected_folder
print("current folder: ", current_folder)
folders, files = list_directory(current_folder)
current_folder_name = os.path.basename(current_folder) or BASE_DIR

# 📁 Folder dropdown
if folders:
    selected_subfolder = st.sidebar.selectbox(
        f"Inside: {current_folder_name}",
        options=[current_folder_name] + folders,
        index=0,
    )
    # If user chooses a subfolder
    if selected_subfolder != current_folder_name:
        st.session_state.selected_folder = os.path.join(current_folder, selected_subfolder)
        st.session_state.selected_file = None  # close preview when navigating
        st.rerun()

# 🔙 Go up one level
if st.sidebar.button("⬆️ Go Up"):
    parent = os.path.dirname(current_folder)
    if os.path.commonpath([BASE_DIR, parent]) == BASE_DIR:
        st.session_state.selected_folder = parent
        st.session_state.selected_file = None  # close preview when going back
        st.rerun()

st.write(f"📍 **Current Folder:** `{os.path.abspath(current_folder)}`")

# 📜 File list
st.sidebar.markdown("### Files")
for file in files:
    if st.sidebar.button(f"📄 {file}", key=file):
        st.session_state["selected_file"] = os.path.join(current_folder, file)
        st.rerun()

# 🔄 Refresh
if st.sidebar.button("🔄 Refresh Folder"):
    st.rerun()

# --- Main content area ---
if st.session_state.selected_file:
    file_path = st.session_state.selected_file
    st.subheader(f"📖 Preview: `{os.path.basename(file_path)}`")

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
        st.code(content, language="yaml" if file_path.endswith(".yml") else "sql")

    # ❌ Close preview button
    if st.button("Close Preview"):
        st.session_state.selected_file = None
        st.rerun()
else:
    st.info("👈 Select a file from the sidebar to preview its contents.")
