import streamlit as st
from google.cloud import geminidataanalytics
from google.cloud import bigquery
import json
import os
import pandas as pd

# --- PAGE CONFIG ---
st.set_page_config(page_title="NRL Stats AI", page_icon="🏆", layout="wide")

# 1. AUTHENTICATION
if "GOOGLE_CREDENTIALS" in st.secrets:
    try:
        creds_data = st.secrets["GOOGLE_CREDENTIALS"]
        creds_dict = json.loads(creds_data) if isinstance(creds_data, str) else dict(creds_data)
        with open("temp_key.json", "w") as f:
            json.dump(creds_dict, f)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "temp_key.json"
    except Exception as e:
        st.error(f"❌ Authentication Error: {e}")
        st.stop()

# --- CONSTANTS ---
MY_PROJECT = "nrl-2026-489302"
MY_LOCATION = "global"
MY_DATASET = "player_stats"
MY_TABLE = "player_stats_test_2"
FULL_TABLE_ID = f"{MY_PROJECT}.{MY_DATASET}.{MY_TABLE}"

# --- SIDEBAR ---
with st.sidebar:
    st.title("🏆 NRL AI Settings")
    if st.button("Clear Chat History"):
        st.session_state.messages = []
        st.rerun()

st.title("🏆 NRL Stats AI Agent")

if "messages" not in st.session_state:
    st.session_state.messages = []

# Display history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

def run_nrl_summary_agent(prompt: str) -> str:
    """Use Gemini Data Analytics ONLY for narrative summary."""
    client = geminidataanalytics.DataChatServiceClient()

    bq_ref = geminidataanalytics.BigQueryTableReference(
        project_id=MY_PROJECT, dataset_id=MY_DATASET, table_id=MY_TABLE
    )

    context = geminidataanalytics.Context(
        system_instruction=(
            "You are an expert NRL analyst using a stats table. "
            "Answer in natural language only. "
            "Do NOT describe your SQL, process, or thinking. "
            "Write a short 'Summary' section and an 'Insights' section. "
            "Finish with 2–3 suggested follow up questions on separate lines."
        ),
        datasource_references=geminidataanalytics.DatasourceReferences(
            bq=geminidataanalytics.BigQueryTableReferences(
                table_references=[bq_ref]
            )
        ),
    )

    request = geminidataanalytics.ChatRequest(
        parent=f"projects/{MY_PROJECT}/locations/{MY_LOCATION}",
        inline_context=context,
        messages=[
            geminidataanalytics.Message(
                user_message=geminidataanalytics.UserMessage(text=prompt)
            )
        ],
    )

    stream = client.chat(request=request)

    parts = []
    for reply in stream:
        if hasattr(reply, "system_message") and getattr(reply.system_message, "text", None):
            parts.extend([p for p in reply.system_message.text.parts if p])

    text = "\n".join(parts).strip()
    if not text:
        return "I could not generate a summary from the data."

    # Split suggested follow-ups into own section
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    main_lines, q_lines = [], []
    for ln in lines:
        if ln.endswith("?"):
            q_lines.append(ln)
        else:
            main_lines.append(ln)

    main_text = "\n\n".join(main_lines)
    followup_text = "\n".join(f"- {q}" for q in q_lines)

    if q_lines:
        return f"{main_text}\n\n\n### Suggested follow up questions\n\n{followup_text}"
    return main_text

def query_runs_vs_tries(round_number: int = 1) -> pd.DataFrame:
    """Direct BigQuery query for runs vs tries for a round."""
    bq_client = bigquery.Client(project=MY_PROJECT)

    query = f"""
    SELECT
      player_name,
      runs,
      tries
    FROM `{FULL_TABLE_ID}`
    WHERE round = @round
    ORDER BY runs DESC
    LIMIT 50
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("round", "INT64", round_number)]
    )

    df = bq_client.query(query, job_config=job_config).to_dataframe()
    return df

# --- CHAT INPUT ---
if prompt := st.chat_input("Ask about NRL Round 1 stats, e.g. 'Show me a chart of runs vs tries for Round 1'"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing NRL stats..."):
            try:
                # 1) Get narrative answer from Gemini
                summary_text = run_nrl_summary_agent(prompt)

                # 2) Always try to build a visual for Round 1 runs vs tries
                #    (you can later parse the prompt to choose round/metric)
                try:
                    df = query_runs_vs_tries(round_number=1)
                except Exception as e:
                    st.error(f"Failed to query BigQuery: {e}")
                    df = None

                if df is not None and not df.empty:
                    st.subheader("Runs vs tries – Round 1 (Top 50 players)")
                    # Ensure numeric
                    for col in ["runs", "tries"]:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors="coerce")

                    num_cols = df.select_dtypes(include=["number"]).columns.tolist()
                    if {"runs", "tries"}.issubset(num_cols):
                        st.scatter_chart(df, x="runs", y="tries", color=None)
                    elif len(num_cols) >= 2:
                        st.scatter_chart(df, x=num_cols[0], y=num_cols[1], color=None)
                    elif num_cols:
                        metric = num_cols[0]
                        chart_df = df.set_index("player_name")[metric] if "player_name" in df.columns else df[metric]
                        st.bar_chart(chart_df)

                    st.dataframe(df, use_container_width=True)
                else:
                    st.info(
                        "No rows were returned for runs vs tries in Round 1. "
                        "Check the table name, dataset, or round filter."
                    )

                # 3) Render text answer
                st.markdown(summary_text)
                st.session_state.messages.append({"role": "assistant", "content": summary_text})

            except Exception as e:
                st.error(f"❌ System Error: {e}")
