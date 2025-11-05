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

st.title("Snowflake dbt Model Generator")

st.write("""
Upload a **mapping file** (CSV/Excel) and a **Snowflake semantic JSON file**  
to automatically generate dbt models and YAMLs.
""")

repo_dir = "dbt_model_generation"

# File uploads
mapping_file = st.file_uploader("Upload Mapping File", type=["csv", "xlsx"])

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
    st.dataframe(df.head())

# --- 1. Connect to Snowflake ---
connection_parameters = {
    "user": "SHAJAHAN",
    "password": "Shajahan@snowflake_2002",
    "account": "DTJCYHT-WGB03396",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "DATASET",
    "schema": "PUBLIC",
}

options = CompleteOptions(
    temperature=0.2,
    max_tokens=4096,
    guardrails=False
)
session = Session.builder.configs(connection_parameters).create() 

models_folder_path = "dbt_model_generation\models"

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
        print("\n🚀 Starting dbt model generation...")

        all_model_yamls = []
        source_tables = set()

        grouped = df.groupby(['target_schema', 'target_table'])

        for (t_schema, t_table), subdf in grouped:
            source_schema = subdf['source_schema'].iloc[0]
            source_table = subdf['source_table'].iloc[0]
            source_tables.add((source_schema, source_table))

            mapping_details = []
            for _, row in subdf.iterrows():
                source_expr = (
                    row['transformation_logic']
                    if pd.notna(row['transformation_logic']) and row['transformation_logic'].strip()
                    else row['source_column']
                )

                description = (
                    row['description']
                    if 'description' in row and pd.notna(row['description'])
                    else "No description provided"
                )

                test_info = (
                    row['test']
                    if 'test' in row and pd.notna(row['test'])
                    else "none"
                )

                mapping_details.append(
                    f"{row['source_column']} → {row['target_column']} ({row['data_type']}) "
                    f"| logic: {source_expr} | description: {description} | test: {test_info}"
                )

            mapping_str = "\n".join(mapping_details)

            # --- AI Prompt for SQL only ---
            prompt_text = f"""
            You are an expert AI assistant specializing in data modeling, dbt, and analytics engineering for Snowflake.
            Your task is to automatically generate dbt models based on provided metadata, SQL logic, or schema definitions.

            You should:
            Follow dbt best practices for model structure, naming conventions, and documentation.
            1. Generate Snowflake-optimized SQL (use CTEs, proper casting, and efficient warehouse functions).
            2. Make sure the snowflake syntax and function usage is as per snowflake's documentation.
            3. Ensure modularity and lineage — staging models should map cleanly to intermediate and final marts.
            4. Include Jinja and dbt macros where appropriate (e.g., for reusable logic, timestamps, or source references).
            5. Clearly separate source, staging, intermediate, and mart layers following dbt folder structure.
            6. Do not add semicolon at the end of the sql.
            7. Use `{{{{ source('{source_schema}', '{source_table}') }}}}` for source references.
            8. Apply transformation logic when provided.
            9. Rename columns as per target mapping.
            10. SQL must start directly with WITH.
            11. Give only sql text no extra text like "```sql" or any other markigs
            12. Use proper intendation
            13. Add a header block in the model as comment with details like the model name, description, created date and Author.
            14. Keep the Author name as "AI Generated" and the created date as current date(IST).
            15. Enclose the comment using "{{# #}}"

            Your output should include:
            1. The dbt model SQL file content (formatted).
            2. (Optional) Add comments wherever necessary.

            Mapping:
            {mapping_str}

            Target Schema: {t_schema}
            Target Table: {t_table}

            Output: SQL only, no markdown or YAML.
            """

            result = Complete(model="claude-4-sonnet", prompt=prompt_text, session=session, options=options)
            result_text = result.get("response") if isinstance(result, dict) else str(result)
            sql_part = re.sub(r"(?i)^sure.*|^here.*|```.*", "", result_text).strip()

            model_path = f"models/{t_table}.sql"
            with open(model_path, "w", encoding="utf-8") as f:
                f.write(sql_part)

            print(f"✅ Model created: {model_path}")
            st.write(f"✅ Model created: `{t_table}.sql`")

        # --- Consolidated schema.yml generation via Cortex ---
        st.write("🧠 Generating consolidated schema.yml with Cortex...")
        print("\n🧠 Starting final Cortex call for schema.yml...")

        model_summary = []
        for (t_schema, t_table), subdf in df.groupby(['target_schema', 'target_table']):
            mapping_lines = []
            for _, row in subdf.iterrows():
                logic = (
                    row['transformation_logic']
                    if pd.notna(row['transformation_logic']) and row['transformation_logic'].strip()
                    else row['source_column']
                )
                mapping_lines.append(
                    f"{row['source_column']} → {row['target_column']} ({row['data_type']}) | logic: {logic}"
                )
            model_summary.append(f"Model: {t_table}\nSchema: {t_schema}\nColumns:\n" + "\n".join(mapping_lines))
        print("models: ", model_summary)

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

        The generated yaml file should look like this:
        version: 2

        models:
          - name: {{ model_name }}
            description: {{ add some description about the model }}
            columns:
              - name: {{column1}}
                description: {{ add some description about the column }}
                tests:
                  {{tests}}


        Models:
        {chr(10).join(model_summary)}

        Output YAML only — no markdown, no code fences, no prose.
        """

        schema_result = Complete(model="claude-4-sonnet", prompt=schema_prompt, session=session, options=options,)
        schema_text = schema_result.get("response") if isinstance(schema_result, dict) else str(schema_result)
        schema_text = re.sub(r"(?i)^sure.*|^here.*|```.*", "", schema_text).strip()

        schema_yml_path = "models/schema.yml"
        with open(schema_yml_path, "w", encoding="utf-8") as f:
            f.write(schema_text)

        print(f"✅ Final consolidated schema.yml generated: {schema_yml_path}")
        st.write("✅ Final consolidated `schema.yml` generated by Cortex AI")

        # --- Consolidated sources.yml generation via Cortex ---
        st.write("🧠 Generating consolidated sources.yml with Cortex...")
        print("\n🧠 Starting final Cortex call for sources.yml...")

        source_summary = []
        for (schema, table), subdf in df.groupby(['source_schema', 'source_table']):
            database = subdf['database'].iloc[0] if 'database' in subdf.columns else "UNKNOWN_DB"
            cols = subdf['source_column'].unique().tolist()
            source_summary.append(
                f"Database: {database}\nSchema: {schema}\nTable: {table}\nColumns: {', '.join(cols)}"
            )
        
        sources_prompt = f"""
        You are a Snowflake + dbt expert.

        Generate a single **sources.yml** file for the following source tables.
        - Follow dbt best practices
        - Output must be valid YAML



        Sources:
        {chr(10).join(source_summary)}

        Output YAML only — no markdown or prose.
        """

        sources_result = Complete(model="claude-4-sonnet", prompt=sources_prompt, session=session, options=options)
        sources_text = sources_result.get("response") if isinstance(sources_result, dict) else str(sources_result)
        sources_text = re.sub(r"(?i)^sure.*|^here.*|```.*", "", sources_text).strip()

        sources_yml_path = "models/sources.yml"
        with open(sources_yml_path, "w", encoding="utf-8") as f:
            f.write(sources_text)

        print(f"✅ Final consolidated sources.yml generated: {sources_yml_path}")
        st.write("✅ Final consolidated `sources.yml` generated by Cortex AI")

        st.success("🎉 All DBT files successfully created under `generated_dbt/`")
        print("\n🎉 All DBT files successfully created under generated_dbt/")

        # Stage all changes
        subprocess.run(["git", "add", "."], check=True)

        # Commit with timestamp
        commit_msg = f"Auto commit - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        subprocess.run(["git", "commit", "-m", commit_msg])

        # Push to branch
        subprocess.run(["git", "push", "origin", "main"])

        print("✅ Changes pushed successfully!")
