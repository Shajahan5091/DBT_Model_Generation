import streamlit as st
import pandas as pd
import yaml
import os
import zipfile
from jinja2 import Environment, FileSystemLoader
import io
import json
from snowflake.snowpark import Session
from snowflake.cortex import Complete, CompleteOptions
import re

st.title("Snowflake dbt Model Generator")

st.write("""
Upload a **mapping file** (CSV/Excel) and a **Snowflake semantic JSON file**  
to automatically generate dbt models and YAMLs.
""")

# File uploads
mapping_file = st.file_uploader("Upload Mapping File", type=["csv", "xlsx"])
semantic_file = st.file_uploader("Upload Snowflake Semantic File (JSON)", type=["json"])

# Create environment for templates
env = Environment(loader=FileSystemLoader("templates"))

if mapping_file and semantic_file:
    # Load mapping file
    if mapping_file.name.endswith(".csv"):
        df = pd.read_csv(mapping_file)
    else:
        df = pd.read_excel(mapping_file)

    semantic = json.load(semantic_file)

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
    temperature=0.4,
    max_tokens=800,
    guardrails=False
)
session = Session.builder.configs(connection_parameters).create() 

# --- 3. Create folder structure ---
os.makedirs("generated_dbt/models", exist_ok=True)
os.makedirs("generated_dbt/sources", exist_ok=True)

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
            You are a Snowflake + dbt expert.

            Generate only one output: **DBT Model SQL**
            - Use `{{{{ source('{source_schema}', '{source_table}') }}}}` for source references.
            - Apply transformation logic when provided.
            - Rename columns as per target mapping.
            - SQL must start directly with WITH.
            - Give only sql text no extra text like "```sql" or any other markigs
            - Use proper intendation

            Mapping:
            {mapping_str}

            Target Schema: {t_schema}
            Target Table: {t_table}

            Output: SQL only, no markdown or YAML.
            """

            result = Complete(model="mistral-large", prompt=prompt_text, session=session, options=options)
            result_text = result.get("response") if isinstance(result, dict) else str(result)
            sql_part = re.sub(r"(?i)^sure.*|^here.*|```.*", "", result_text).strip()

            model_path = f"generated_dbt/models/{t_table}.sql"
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

        schema_prompt = f"""
        You are a Snowflake + dbt expert.

        Generate one valid dbt **schema.yml** file for the following models.
            - Describe the model `{t_table}` with columns, descriptions, and dbt tests.
            - Follow dbt YAML structure starting with `version: 2` and `models:`.
            - Use only valid YAML (no markdown formatting).
            - Don't repeat the words like `version` or `models`.
            - Use proper intendation

        Models:
        {chr(10).join(model_summary)}

        Output YAML only — no markdown, no code fences, no prose.
        """

        schema_result = Complete(model="mistral-large", prompt=schema_prompt, session=session, options=options)
        schema_text = schema_result.get("response") if isinstance(schema_result, dict) else str(schema_result)
        schema_text = re.sub(r"(?i)^sure.*|^here.*|```.*", "", schema_text).strip()

        schema_yml_path = "generated_dbt/models/schema.yml"
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
        Each source should include:
        - version: 2
        - name, database, schema, tables, and columns
        - Add minimal tests (e.g., not_null or unique)
        - Output must be valid YAML

        Sources:
        {chr(10).join(source_summary)}

        Output YAML only — no markdown or prose.
        """

        sources_result = Complete(model="mistral-large", prompt=sources_prompt, session=session, options=options)
        sources_text = sources_result.get("response") if isinstance(sources_result, dict) else str(sources_result)
        sources_text = re.sub(r"(?i)^sure.*|^here.*|```.*", "", sources_text).strip()

        sources_yml_path = "generated_dbt/sources/sources.yml"
        with open(sources_yml_path, "w", encoding="utf-8") as f:
            f.write(sources_text)

        print(f"✅ Final consolidated sources.yml generated: {sources_yml_path}")
        st.write("✅ Final consolidated `sources.yml` generated by Cortex AI")

        st.success("🎉 All DBT files successfully created under `generated_dbt/`")
        print("\n🎉 All DBT files successfully created under generated_dbt/")
