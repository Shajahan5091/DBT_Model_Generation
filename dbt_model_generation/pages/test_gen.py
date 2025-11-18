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

apply_style()
render_header()

st.set_page_config(page_title="dbt Model Generator", layout="wide")
st.title("Snowflake dbt Model Generator")

st.write("""
Upload a **mapping file** (CSV/Excel)  
to automatically generate dbt models and YAMLs.
""")

repo_dir = "dbt_model_generation"

# File uploads
mapping_file = st.file_uploader("Upload Mapping File", type=["csv", "xlsx"])

# TO BE PART OF LATER SCOPE
semantic_file = st.file_uploader("Upload Snowflake Semantic File (JSON)", type=["yaml"])

# Create environment for templates
env = Environment(loader=FileSystemLoader("templates"))

if mapping_file and semantic_file:
    # Load mapping file
    if mapping_file.name.endswith(".csv"):
        df = pd.read_csv(mapping_file)
    else:
        df = pd.read_excel(mapping_file)

    semantic_yaml = yaml.safe_load(semantic_file)
    st.success("✅ Semantic file loaded successfully!")
    with st.expander("🔍 Preview Semantic YAML", expanded=False):
        st.json(semantic_yaml)

    # semantic = json.load(semantic_file)

    st.subheader("📋 Mapping Preview")
    st.dataframe(df.head())

# --- 1. Connect to Snowflake ---
connection_parameters = {
    "user": "SHAJAHAN",
    "password": "Shajahan@snowflake_2002",
    "account": "HPAFNDX-IW71152",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "BSL_RAW",
    "schema": "DWH_RAW",
}

options = CompleteOptions(
    temperature=0.2,
    max_tokens=4096,
    guardrails=False
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

models_folder_path = r"C:\Users\Sashanth.Omprakash\DBT_Model_Generation\dbt_model_generation\Sash_test\models" 

# --- 3. Create folder structure ---
if os.path.exists(models_folder_path):
    shutil.rmtree(models_folder_path)
    print(f"🗑️ Deleted existing folder: {models_folder_path}")

# Create new folder
os.makedirs(models_folder_path)

# --- Execute creation of Models and dbt Artifacts ---
if st.button("Generate dbt Models"):

    if mapping_file is None:
        st.error("⚠️ Please upload a Mapping File before generating dbt models.")
    else:
        st.write("🚀 Generating dbt models... please wait.")
        semantic_text = yaml.dump(semantic_yaml)
        st.write("🚀 REdaing Mapping doc and Semantic files... please wait.")
        print("\n🚀 Starting dbt model generation...")

        grouped = df.groupby(['Target_Database', 'Target_Schema', 'Target_Table'])

        Source_Tables = set()

        for (t_database, t_schema, t_table), subdf in grouped:
    
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
                Incremental_Where_Condition = row.get('Incremental_Where_Condition', '')

                mapping_details.append(
                    f"{Source_Database} {Source_Schema} {Source_Table} {Source_Column} ({source_datatype}) "
                    f"→ {t_database} {t_schema} {target_table} {target_column} ({target_datatype}) "
                    f"| logic: {mapping_rule} | notes: {notes} | description: {description} | test: {test_info} | Materialization: {Materialization} | Incremental_Where_Condition: {Incremental_Where_Condition}"
                )

            # ✅ Combine mapping once per table
            mapping_str = "\n".join(mapping_details)
            print('mapping details:\n',mapping_str)

            # ✅ Call AI once per table (NOT inside row loop)
            model_prompt_text = f"""
            You are an expert AI assistant specializing in data modeling, dbt, and analytics engineering for Snowflake.
            Your task is to automatically generate dbt models based on provided metadata.

            Validate the mapping document against the provided semantic model before generating dbt models.
            In case of Column name or data type errors while comparing Mapping document and snowflake semantic data , 
            auto correct it based on semantic file and make sure no error occurs.
            When ever you do such auto correct add a comment nearby saying auto corrected using semantic file.

             **Semantic Model (YAML):**
            ```
            {semantic_text}
            ```

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
            Keep the Author name as "AI Generated by Shajahan" and the created date as current date(IST).
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

            result = Complete(model="claude-4-sonnet", prompt=model_prompt_text, session=session, options=options)
            result_text = result.get("response") if isinstance(result, dict) else str(result)
            sql_part = re.sub(r"(?i)^sure.*|^here.*|```.*", "", result_text).strip()

            # ✅ Save the dbt model once
            os.makedirs("Sash_test/models", exist_ok=True)
            model_name = t_table.strip().lower()
            print("Model Name: ", model_name)
            model_path = f"Sash_test/models/{model_name}.sql"
            with open(model_path, "w", encoding="utf-8") as f:
                f.write(sql_part)

            print(f"✅ Model created successfully → {model_path}")
            st.write(f"✅ Model created: `{t_table}.sql`")

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
                    f"{row['Source_Column']} → {row['Target_Column']} ({row['Target_Data_Type']}) | logic: {logic} | Test: {test}"
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
            - Add only provided tests.

        The generated yaml file should look like this:
        {st.session_state.get('schema', '')}

        Models:
        {chr(10).join(model_summary)}

        Output YAML only — no markdown, no code fences, no prose.
        """

        schema_result = Complete(model="claude-4-sonnet", prompt=schema_prompt, session=session, options=options,)
        schema_text = schema_result.get("response") if isinstance(schema_result, dict) else str(schema_result)
        schema_text = re.sub(r"(?i)^sure.*|^here.*|```.*", "", schema_text).strip()

        schema_yml_path = "Sash_test/models/models.yml"
        with open(schema_yml_path, "w", encoding="utf-8") as f:
            f.write(schema_text)

        print(f"✅ Final consolidated models.yml generated: {schema_yml_path}")
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

        sources_result = Complete(model="claude-4-sonnet", prompt=sources_prompt, session=session, options=options)
        sources_text = sources_result.get("response") if isinstance(sources_result, dict) else str(sources_result)
        sources_text = re.sub(r"(?i)^sure.*|^here.*|```.*", "", sources_text).strip()

        sources_yml_path = "Sash_test/models/sources.yml"
        with open(sources_yml_path, "w", encoding="utf-8") as f:
            f.write(sources_text)

        print(f"✅ Final consolidated sources.yml generated: {sources_yml_path}")
        st.write("✅ Final consolidated `sources.yml` generated by Cortex AI")

        print("\n🎉 All dbt files successfully created under dbt_model_generation/")
        st.success("🎉 All dbt files successfully created under `dbt_model_generation/`")

        # # Stage all changes
        # subprocess.run(["git", "add", "."], check=True)

        # # Commit with timestamp
        # commit_msg = f"Auto commit - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        # subprocess.run(["git", "commit", "-m", commit_msg])

        # # Push to branch
        # subprocess.run(["git", "push", "origin", "main"])

        # print("✅ Changes pushed successfully!")
