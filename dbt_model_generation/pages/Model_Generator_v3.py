import streamlit as st
import pandas as pd
import yaml
import os
import shutil
from jinja2 import Environment, FileSystemLoader
import json
from snowflake.snowpark import Session
from snowflake.cortex import Complete, CompleteOptions
import re
import subprocess
from datetime import datetime
from style_utils import apply_style, render_header
from github import Github
import base64
from github import GithubException
import time


apply_style()
render_header()

st.set_page_config(page_title = "dbt Model Generator", layout = "wide")
st.title("Snowflake dbt Model Generator")

st.write("""
Generate dbt model files using AI, review them, and automatically push them 
to your GitHub repository.  
Provide your GitHub credentials below to connect securely using a Personal Access Token (PAT) 
with `repo` permissions.
""")

# ---------------------------------------------
# SESSION INITIALIZATION
# ---------------------------------------------
defaults = {
    "github_authenticated": False,
    "github_user": "",
    "github_token": "",
    "repo_name": "",
    "branch_name": "",
    "show_expander": True,  # Controls expansion
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ---------------------------------------------
# GITHUB AUTH SECTION
# ---------------------------------------------
st.subheader("🔐 GitHub Connection")

with st.expander("🔐 Connect to GitHub", expanded = st.session_state.show_expander):
    col1, col2 = st.columns(2)

    with col1:
        username = st.text_input(
            "GitHub Username",
            value = st.session_state.github_user,
            key = "github_username_input"
        )

        repo_name = st.text_input(
            "Repository Name (e.g., username/repo_name)",
            key = "repo_name_input",
            value = st.session_state.repo_name
        )

    with col2:
        github_token = st.text_input(
            "GitHub Personal Access Token (PAT)",
            type = "password",
            key = "github_token_input",
            value = st.session_state.github_token
        )

        branch_name = st.text_input(
            "Branch name (default auto-created)",
            value = st.session_state.branch_name,
            key = "branch_name_input"
        )
    connect_btn = st.button("🔗 Connect to GitHub")

    if connect_btn:
        if not username or not github_token or not repo_name:
            st.error("Please enter username, token, and repository name.")
        else:
            if "/" not in repo_name:
                repo_full_name = f"{username}/{repo_name}"
            else:
                repo_full_name = repo_name

            with st.spinner("🔄 Connecting to GitHub..."):
                time.sleep(1.5)  # simulate delay
                try:
                    g = Github(github_token)
                    user = g.get_user()
                    # If token works, user.login should succeed
                    st.session_state.github_authenticated = True
                    st.session_state.github_user = user.login
                    st.session_state.github_token = github_token
                    st.session_state.repo_name = repo_full_name
                    st.session_state.branch_name = branch_name
                    st.session_state.repo = g.get_repo(repo_full_name)
                    st.session_state.show_expander = False  # collapse after success
                    st.success(f"✅ Connected successfully as **{user.login}**")

                except GithubException as e:
                    st.error(f"❌ GitHub authentication failed: {e}")
                except Exception as ex:
                    st.error(f"⚠️ Error connecting to GitHub: {ex}")

# ---------------------------------------------
# CONNECTION STATUS
# ---------------------------------------------
if st.session_state.github_authenticated:
    st.markdown(
        f"<p style='color:green;'>🔗 Connected as <b>{st.session_state.github_user}</b></p>",
        unsafe_allow_html=True
    )
else:
    st.info("🔸 Not connected to GitHub yet. Expand the section above to connect.")


# ---------------------------------------------
# GITHUB AUTH SECTION
# ---------------------------------------------
st.subheader("Model Generation")

st.write("""
Upload a **mapping file** (CSV/Excel) to automatically generate dbt models and YAMLs.
""")

repo_dir = "dbt_model_generation"

# File uploads
mapping_file = st.file_uploader("Upload Mapping File", type = ["csv", "xlsx"])

# TO BE PART OF LATER SCOPE
#  semantic_file = st.file_uploader("Upload Snowflake Semantic File (JSON)", type=["json"])

# Create environment for templates
env = Environment(loader=FileSystemLoader("templates"))

if mapping_file:  # and semantic_file:
    # Load mapping file
    if mapping_file.name.endswith(".csv"):
        df = pd.read_csv(mapping_file)
    else:
        df = pd.read_excel(mapping_file)

    # semantic = json.load(semantic_file)

    st.subheader("📋 Mapping Preview")
    st.dataframe(df)

# --- 1. Connect to Snowflake ---
connection_parameters = {
    "user": "SHAJAHAN",
    "password": "Shajahan@snowflake_2002",
    "account": "YVCFONH-QR62969",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "BSL_MA",
    "schema": "DWH_MA",
}

options = CompleteOptions(
    temperature = 0.2,
    max_tokens = 4096,
    guardrails = False
)
session = Session.builder.configs(connection_parameters).create() 

# -- Get the required documents & templates from snowflake table
st.session_state.schema = session.sql(
        f"select RAW_TEXT from SNOWFLAKE_LEARNING_DB.DOCS_TEST.TEMPLATES where file_name = 'schema.txt'"
    ).collect()

st.session_state.sources = session.sql(
        f"select RAW_TEXT from SNOWFLAKE_LEARNING_DB.DOCS_TEST.TEMPLATES where file_name = 'sources.txt'"
    ).collect()

st.session_state.model = session.sql(
        f"select RAW_TEXT from SNOWFLAKE_LEARNING_DB.DOCS_TEST.TEMPLATES where file_name = 'model.txt'"
    ).collect()

st.session_state.sf_best = session.sql(
        f"select RAW_TEXT from SNOWFLAKE_LEARNING_DB.DOCS_TEST.STATIC_DOCS where file_name = 'Snowflake_Best_Practices_V1.1.docx'"
    ).collect()

st.session_state.dbt_best = session.sql(
        f"select RAW_TEXT from SNOWFLAKE_LEARNING_DB.DOCS_TEST.STATIC_DOCS where file_name = 'dbt best practices_v01.docx'"
    ).collect()

# g = Github(st.session_state.github_token)
# repo = g.get_repo(st.session_state.repo_name)

# --- Execute creation of Models and dbt Artifacts ---
if st.button("Generate dbt Models"):

    repo = st.session_state.repo
    if mapping_file is None:
        st.error("⚠️ Please upload a Mapping File before generating dbt models.")
    else:
        st.write("🚀 Generating dbt models... please wait.")
        print("\n🚀 Starting dbt model generation...")

        grouped = df.groupby(['Target_Database', 'Target_Schema', 'Target_Table'])

        Source_Tables = set()

        for (t_database, t_schema, t_table), subdf in grouped:
        
            # Source_Database = subdf['Source_Database'].iloc[0]
            # Source_Schema = subdf['Source_Schema'].iloc[0]
            # Source_Table = subdf['Source_Table'].iloc[0]
            # Source_Tables.add((Source_Schema, Source_Table))

            # print("\n🔹 Processing Model:", t_table)
            # print("   Source_Database:", Source_Database)
            # print("   Source_Schema  :", Source_Schema)
            # print("   Source_Table   :", Source_Table)

            mapping_details = []

            # ✅ Loop rows only to collect mapping details
            for _, row in subdf.iterrows():
                Source_Database = row.get('Source_Database', '')
                Source_Schema = row.get('Source_Schema', '')
                Source_Table = row.get('Source_Table', '')
                Source_Column = row.get('Source_Column', '')
                target_table = row.get('Target_Table', '')
                target_column = row.get('Target_Column', '')
                source_datatype = row.get('Source_Data_Type', '')
                target_datatype = row.get('Target_Data_Type', '')
                mapping_rule = row.get('Mapping_Rule', Source_Column)
                description = row.get('Table_Description', "No description provided")
                test_info = row.get('Test', "none")
                notes = row.get('Notes', '')
                Materialization = row.get('Materialization', '')
                # Incremental_Where_Condition = row.get('Incremental_Where_Condition', '')

                mapping_details.append(
                    f"{Source_Database} {Source_Schema} {Source_Table} {Source_Column} ({source_datatype}) "
                    f"→ {t_database} {t_schema} {target_table} {target_column} ({target_datatype}) "
                    f"| logic: {mapping_rule} | notes: {notes} | description: {description} | test: {test_info} | Materialization: {Materialization}"
                )

            # ✅ Combine mapping once per table
            mapping_str = "\n".join(mapping_details)
            print('mapping details:\n', mapping_str)

            # ✅ Call AI once per table (NOT inside row loop)
            model_prompt_text = f"""
            You are an expert AI assistant specializing in data modeling, dbt, and analytics engineering for Snowflake.
            Your task is to automatically generate dbt models based on provided metadata.

            Follow dbt & Snowflake best practices.
            Do not include markdown formatting.
            Add comments as required.
            Utilize the Mapping_Rule and notes given below and transform accordingly.
            Use sorce reference as per dbt guidlines. For source name use {Source_Schema} in lowercase. for model name use {Source_Table} in lowercase.
            Use ref when referring another model or target table.
            Must generate the models as CTEs.
            Do not add semicolon at the end.
            Use proper aliases for the required columns and use prefix properly.
            Always use prefix for all the columns if join is used.
            The naming of aliases should be proper name instead of just giving single letter names. Pls refer the below best practices for reference.

            If the materialization is incremental then add another column "last_updated" in the model with current timestamp as default value.
            Add the incremental condition - "{{% if is_incremental() %}} where last_updated > (select max(last_updated) from {{ this }}) {{% endif %}}".

            Add a header block in the model as comment with details like the below template
            ------------------------------------------------------------------------
            MODEL NAME    : 
            TARGET TABLE  : 
            SOURCE TABLES : 
            DESCRIPTION   : 
            PREREQUISITES : 
            PARAMETER     : 
            AUTHOUR       : 
            ------------------------------------------------------------------------
            model name shoud be the target table name in the header block
            target table name should be the fully qualified name (as per snowflake standards) of the model.
            Source tables are the source names that the model is referring to (include all the source names). Give as snowflake's fully qualified table names. Get the table name from the mapping details give below.
            Give some description about the model.
            Provide prerequisites if any and similarly for parameter.
            Keep the Author name as "AI Generated by Shajahan - {datetime.now().strftime('%Y-%m-%d')}" .
            Enclose the comment using "{{# #}}".
            Align and justify the content of this header.

            Mapping:
            {mapping_str}

            Target Database: {t_database}
            Target Schema: {t_schema}
            Target Table: {t_table}

            The generated sql file should look like this:
            {st.session_state.get('model', '')}

            Utilize the below best practices and naming conventions wherever required.
            Snowflake best Practices: {st.session_state.get('sf_best', '')}
            dbt best practices: {st.session_state.get('dbt_best', '')}

            Output only SQL.
            """

            result = Complete(model = "claude-4-sonnet", prompt = model_prompt_text, session = session, options = options)
            result_text = result.get("response") if isinstance(result, dict) else str(result)
            sql_part = re.sub(r"(?i)^sure.*|^here.*|```.*", "", result_text).strip()

            # ✅ write model file in repo
            model_name = t_table.strip().lower()
            print("Model Name: ", model_name)
            model_path = f"dbt_model_generation/models/{model_name}.sql"
            model_sql_message = f"Auto commit - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            try:
                file = repo.get_contents(model_path)
                # File exists → update it
                repo.update_file(
                    model_path, model_sql_message, sql_part, file.sha, branch = st.session_state.branch_name
                )
                print("File updated!")
            except:
                # File does not exist → create it
                repo.create_file(
                    model_path, model_sql_message, sql_part, branch = st.session_state.branch_name
                )

            print(f"✅ Model created successfully → {model_path}")
            st.write(f"✅ Model created: `{model_name}.sql`")

        st.write("🧠 Generating consolidated models.yml with Cortex...")
        # --- Consolidated schema.yml generation via Cortex ---
        model_summary = []
        for (t_database, t_schema, t_table), subdf in grouped:
            mapping_lines = []
            for _, row in subdf.iterrows():
                logic = (
                    row['Mapping_Rule']
                    if pd.notna(row['Mapping_Rule']) and row['Mapping_Rule'].strip()
                    else row['Source_Column']
                )
                test = (
                    row['Test']
                    if pd.notna(row['Test']) and row['Test'].strip()
                    else row['Test']
                )
                mapping_lines.append(
                    f"{row['Source_Column']} → {row['Target_Column']} ({row['Target_Data_Type']}) | logic: {logic}"
                )
            model_summary.append(f"Model: {t_table}\nSchema: {t_schema}\nColumns:\n" + "\n".join(mapping_lines))
        # print(model_summary)

        schema_prompt = f"""
        You are a Snowflake + dbt expert.

        Generate one valid dbt **schema.yml** file for the following models.
            - Describe the model `{{t_table}}` with columns, descriptions, and dbt tests.
            - Follow dbt YAML structure starting with `version: 2` and `models:`.
            - Use only valid YAML (no markdown formatting).
            - Don't repeat the words like `version` or `models`.
            - Use proper intendation
            - Don't skip anything. Give full code.
            - Don't add schema name and type.
            - automatically add appropriate dbt & dbt-utils tests based on each column’s name, datatype, and nullability

        The generated yaml file should look like this:
        {st.session_state.get('schema', '')}

        Models:
        {chr(10).join(model_summary)}

        Output YAML only — no markdown, no code fences, no prose.
        """

        schema_result = Complete(model = "claude-4-sonnet", prompt = schema_prompt, session = session, options = options,)
        schema_text = schema_result.get("response") if isinstance(schema_result, dict) else str(schema_result)
        schema_text = re.sub(r"(?i)^sure.*|^here.*|```.*", "", schema_text).strip()

        model_yml_path = "dbt_model_generation/models/models.yml"
        model_yml_message = f"Auto commit - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        try:
            file = repo.get_contents(model_yml_path)
            # File exists → update it
            repo.update_file(
                model_yml_path, model_yml_message, schema_text, file.sha, branch = st.session_state.branch_name
            )
            print("File updated!")
        except:
            # File does not exist → create it
            repo.create_file(
                model_yml_path, model_yml_message, schema_text, branch = st.session_state.branch_name
            )

        print(f"✅ Final consolidated models.yml generated: {model_yml_path}")
        st.write("✅ Final consolidated `models.yml` generated by Cortex AI")

        
        st.write("🧠 Generating consolidated sources.yml with Cortex...")
        source_summary = []
        cols = []
        for (schema, table), subdf in df.groupby(['Source_Schema', 'Source_Table']):
            database = subdf['Source_Database'].iloc[0]
            for val in subdf['Source_Column'].dropna().astype(str):
                parts = [p.strip() for p in val.split(',')]
                cols.extend(parts)

            cols = list(set(cols))
            cols = subdf['Source_Column'].dropna().unique().tolist()

            for _, row in subdf.iterrows():
                logic = (
                    row['Mapping_Rule']
                    if pd.notna(row['Mapping_Rule']) and row['Mapping_Rule'].strip()
                    else ''
                )
                notes = (
                    row['Notes']
                    if pd.notna(row['Notes']) and row['Notes'].strip()
                    else ''
                )
            source_summary.append(
                f"Database: {database}\nSchema: {schema}\nTable: {table}\nColumns: {', '.join(cols)}\nMapping_Rule: {logic}\nNotes: {notes}"
            )
            # print(source_summary)

        sources_prompt = f"""
        You are a Snowflake + dbt expert.

        Generate a single **sources.yml** file for the following source tables.
        - Follow dbt best practices
        - Output must be valid YAML
        - keep the source names as {Source_Schema} in lowercase
        - Look at the mapping_rule and notes for additional source details and create those sources if not already created.

        The generated yaml file should look like this:
        {st.session_state.get('sources', '')}

        Sources:
        {chr(10).join(source_summary)}

        Output YAML only — no markdown or prose.
        """

        sources_result = Complete(model = "claude-4-sonnet", prompt = sources_prompt, session = session, options = options)
        sources_text = sources_result.get("response") if isinstance(sources_result, dict) else str(sources_result)
        sources_text = re.sub(r"(?i)^sure.*|^here.*|```.*", "", sources_text).strip()

        sources_yml_path = "dbt_model_generation/models/sources.yml"
        sources_yml_message = f"Auto commit - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        try:
            file = repo.get_contents(sources_yml_path)
            # File exists → update it
            repo.update_file(
                sources_yml_path, sources_yml_message, sources_text, file.sha, branch = st.session_state.branch_name
            )
            print("File updated!")
        except:
            # File does not exist → create it
            repo.create_file(
                sources_yml_path, sources_yml_message, sources_text, branch = st.session_state.branch_name
            )

        print(f"✅ Final consolidated sources.yml generated: {sources_yml_path}")
        st.write("✅ Final consolidated `sources.yml` generated by Cortex AI")

        print("\n🎉 All dbt files successfully created under models/")
        st.success("🎉 All dbt files successfully created under `models/`")

        profiles_yml_path = "dbt_model_generation/profiles.yml"
        profiles_yml_message = f"Auto commit - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        profiles_content = """dbt_model_generation:
    outputs:
      dev:
        account: YVCFONH-QR62969
        database: BSL_MA
        password: Shajahan@snowflake_2002
        role: ACCOUNTADMIN
        schema: DWH_MA
        threads: 1
        type: snowflake
        user: SHAJAHAN
        warehouse: COMPUTE_WH
    target: dev
        """   
        try:
            file = repo.get_contents(profiles_yml_path)
            # File exists → update it
            repo.update_file(
                profiles_yml_path, profiles_yml_message, profiles_content, file.sha, branch = st.session_state.branch_name
            )
            print("File updated!")
        except:
            # File does not exist → create it
            repo.create_file(
                profiles_yml_path, profiles_yml_message, profiles_content, branch = st.session_state.branch_name
            )

        print(f"✅ The profiles.yml generated: {sources_yml_path}")
        st.write("✅ The profiles.yml generated")

        project_yml_path = "dbt_model_generation/dbt_project.yml"
        project_yml_message = f"Auto commit - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        project_content = """# Name your project! Project names should contain only lowercase characters
    # and underscores. A good package name should reflect your organization's
    # name or the intended use of these models
    name: 'dbt_model_generation'
    version: '1.0.0'
    config-version: 2
    
    # This setting configures which "profile" dbt uses for this project.
    profile: 'dbt_model_generation'

    quoting:
    database: true
    schema: true
    identifier: false
    
    vars:
      dbt_model_generation:
        DWH_TABLE:
          backfill_flag: "false"
          backfill_start_date: "2022-01-01"
          incremental_days: "366"
          backfill_end_date: "2022-01-02"
    
    # These configurations specify where dbt should look for different types of files.
    # The `model-paths` config, for example, states that models in this project can be
    # found in the "models/" directory. You probably won't need to change these!
    model-paths: ["models"]
    analysis-paths: ["analyses"]
    test-paths: ["tests"]
    seed-paths: ["seeds"]
    macro-paths: ["macros"]
    snapshot-paths: ["snapshots"]
    
    target-path: "target"  # directory which will store compiled SQL files
    clean-targets:         # directories to be removed by `dbt clean`
    - "target"
    - "dbt_packages"
    
    # on-run-start: create table if not exists int.DBTExecutionLog (DBTId varchar, DBTRunStartTime TIMESTAMP, DBTRunEndTime TIMESTAMP, DBTStatus varchar, TotalTimeTakenInSeconds int); INSERT INTO int.DBTExecutionLog VALUES ('{{invocation_id}}',current_timestamp(),NULL,'Started',NULL);create table if not exists int.dbtsteplog (invocationid varchar,tablename varchar, stepstart timestamp default current_timestamp(),stepend timestamp ,stepstatus varchar);
    # on-run-end: UPDATE int.DBTExecutionLog SET DBTRunEndTime = current_timestamp(), DBTStatus = 'Successful' WHERE DBTId = '{{invocation_id}}';
    
    # Configuring models
    # Full documentation: https://docs.getdbt.com/docs/configuring-models
    
    # In this example config, we tell dbt to build all models in the example/ directory
    # as tables. These settings can be overridden in the individual model files
    models:
        dbt_model_generation:
        # Config indicated by + and applies to all files under models/example/
            +meta:
            example:
                +materialized: view
        flags:
        require_generic_test_arguments_property: true
        """

        try:
            file = repo.get_contents(project_yml_path)
            # File exists → update it
            repo.update_file(
                project_yml_path, project_yml_message, project_content, file.sha, branch = st.session_state.branch_name
            )
            print("File updated!")
        except:
            # File does not exist → create it
            repo.create_file(
                project_yml_path, project_yml_message, project_content, branch = st.session_state.branch_name
            )

        print(f"✅ The dbt_project.yml generated: {sources_yml_path}")
        st.write("✅ The dbt_project.yml generated")


        

