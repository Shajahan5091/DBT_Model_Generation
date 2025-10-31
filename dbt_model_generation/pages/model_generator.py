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
    "database": "DBT_MODEL_GENERATION",
    "schema": "DBT",
}

options = CompleteOptions(
    temperature=0.4,
    max_tokens=800,
    guardrails=False
)
session = Session.builder.configs(connection_parameters).create() 

# --- 3. Create folder structure ---
os.makedirs("models", exist_ok=True)
os.makedirs("sources", exist_ok=True)

# Execute creation of Models and dbt Artifacts with Button action
if st.button("Generate dbt Models"):

    # --- Validation before button action ---
    if mapping_file is None:
        st.error("⚠️ Please upload a Mapping File before generating dbt models.")
    else:
        st.write("🚀 Generating dbt models... please wait.")
        print("🚀 Starting dbt model generation...")

        # --- Initialize containers ---
        all_model_yamls = []
        source_tables = set()

        # --- 5. Group by Target Table ---
        grouped = df.groupby(['target_schema', 'target_table'])

        # --- 5. Process each model ---
        for (t_schema, t_table), subdf in grouped:
            source_schema = subdf['source_schema'].iloc[0]
            source_table = subdf['source_table'].iloc[0]
            database_name = subdf['database'].iloc[0]

            source_tables.add((database_name, source_schema, source_table))

            # --- Build mapping details ---
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
                    f"| logic: {source_expr} "
                    f"| description: {description} "
                    f"| test: {test_info}"
                )

            mapping_str = "\n".join(mapping_details)

            # --- Prompt for Cortex ---
            prompt_text = f"""
            You are a Snowflake + dbt expert.

            Generate exactly two outputs (no explanations, no markdown, no code fences):

            1 **DBT Model SQL**
               - Use `{{{{ source('{source_schema}', '{source_table}') }}}}` for source reference.
               - Apply transformation logic when provided.
               - Rename columns as per target mapping.
               - Use proper intendation

            2 **DBT Model YAML block** (after a line "---yaml---")
               - Describe the model `{t_table}` with columns, descriptions, and dbt tests.
               - Follow dbt YAML structure starting with `version: 2` and `models:`.
               - Use only valid YAML (no markdown formatting).
               - Use proper intendation

            Schema Mapping:
            {mapping_str}

            Source Schema: {source_schema}
            Source Table: {source_table}
            Target Schema: {t_schema}
            Target Table (DBT Model Name): {t_table}

            Output format:
            <SQL>
            ---yaml---
            <model YAML>

            Important:
            - SQL must start directly with SELECT or WITH.
            - YAML must be valid and indented correctly.
            - Give only sql text no extra text like ```sql or ```yaml. only necessary codes
            """

            # --- 6. Cortex AI call ---
            print(f"\n🧠 Generating model for: {t_table}")
            result = Complete(model="mistral-large", prompt=prompt_text, session=session, options=options)

            result_text = result.get("response") if isinstance(result, dict) else str(result)

            parts = result_text.split("---yaml---", 1)
            sql_part = parts[0].strip()
            yaml_part = parts[1].strip() if len(parts) > 1 else ""

            # --- Clean unwanted prose (heuristic cleanup) ---
            sql_part = re.sub(r"(?i)^sure,.*|^here.*|^based on.*", "", sql_part).strip()

            # --- Write model SQL ---
            model_path = f"models/{t_table}.sql"
            with open(model_path, "w", encoding="utf-8") as f:
                f.write(sql_part)
            print(f"✅ Created model SQL: {model_path}")

            # --- Collect model YAML ---
            all_model_yamls.append(yaml_part)
            print(f"✅ Collected YAML for model: {t_table}")

        # --- End of model loop ---
        st.write("✅ All models and schema.yml blocks generated. Now generating consolidated sources.yml...")
        print("\n🧩 All models generated. Starting final Cortex call for sources.yml...")

        # --- 7. Prepare summary for source.yml ---
        unique_sources = (
            df[['database', 'source_schema', 'source_table']]
            .drop_duplicates()
            .sort_values(by=['database', 'source_schema', 'source_table'])
        )

        source_summary = "\n".join([
            f"Database: {row['database']}, Schema: {row['source_schema']}, Table: {row['source_table']}"
            for _, row in unique_sources.iterrows()
        ])

        # --- 8. Cortex prompt for sources.yml ---
        source_prompt = f"""
        You are a Snowflake + dbt expert.

        Generate a complete dbt **sources.yml** file for the following list of sources.
        Each source must include:
        - version: 2
        - name: schema name
        - database: database name
        - schema: schema name
        - tables: list of all tables under that schema
        - each table must have `name:` and a `description:`
        - if possible, add simple column tests (like unique on id)
        - Combine everything into a single valid YAML (no markdown, no explanations).

        Sources:
        {source_summary}

        Output only valid YAML.
        """

        source_result = Complete(model="mistral-large", prompt=source_prompt, session=session, options=options)
        source_text = source_result.get("response") if isinstance(source_result, dict) else str(source_result)
        source_text = re.sub(r"(?i)^sure.*|^here.*|```yaml|```", "", source_text).strip()

        sources_yml_path = "sources/sources.yml"
        with open(sources_yml_path, "w", encoding="utf-8") as f:
            f.write(source_text)

        print(f"✅ Created final consolidated sources.yml: {sources_yml_path}")
        st.write("✅ Final consolidated `sources.yml` generated by Cortex AI")

        # --- 9. Combine all model YAMLs into schema.yml ---
        schema_yml_path = "models/schema.yml"
        merged_yaml = "version: 2\n\nmodels:\n"

        for yml in all_model_yamls:
            cleaned = re.sub(r"(?im)^version:\s*2\s*", "", yml)
            cleaned = re.sub(r"(?im)^models:\s*", "", cleaned)
            cleaned = re.sub(r"(?m)^(?=\S)", "  ", cleaned)
            merged_yaml += cleaned.strip() + "\n\n"

        with open(schema_yml_path, "w", encoding="utf-8") as f:
            f.write(merged_yaml)

        print(f"✅ Combined schema.yml created: {schema_yml_path}")
        st.write("✅ Combined schema.yml generated (with merged models)")
        st.write("\n🎉 All DBT files successfully created under 'dbt_model_generation/'")

        print("\n🎉 All dbt artifacts successfully generated!\n")
