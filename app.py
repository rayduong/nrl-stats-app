import streamlit as st
from google.cloud import geminidataanalytics
from google.cloud import bigquery
import json
import os
import re
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
MY_PROJECT    = "nrl-2026-489302"
MY_LOCATION   = "global"
MY_DATASET    = "player_stats"
MY_TABLE      = "player_stats_test_2"
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


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def get_schema_string() -> tuple[str, list[str], list[str]]:
    """
    Fetches the real column names and types from BigQuery.
    Returns:
      schema_str   – human-readable column list to inject into prompts
      numeric_cols – column names with numeric BQ types
      all_cols     – all column names
    """
    bq = bigquery.Client(project=MY_PROJECT)
    table = bq.get_table(FULL_TABLE_ID)
    numeric_types = {"INTEGER", "FLOAT", "NUMERIC", "BIGNUMERIC", "INT64", "FLOAT64"}
    all_cols, numeric_cols = [], []
    lines = []
    for f in table.schema:
        all_cols.append(f.name)
        lines.append(f"  {f.name} ({f.field_type})")
        if f.field_type.upper() in numeric_types:
            numeric_cols.append(f.name)
    return "\n".join(lines), numeric_cols, all_cols


def ask_agent(prompt: str, schema_str: str) -> dict:
    """
    Calls Gemini Data Analytics agent.
    Returns {"summary": str, "sql": str | None, "followups": list[str]}
    """
    client = geminidataanalytics.DataChatServiceClient()

    bq_ref = geminidataanalytics.BigQueryTableReference(
        project_id=MY_PROJECT, dataset_id=MY_DATASET, table_id=MY_TABLE
    )
    context = geminidataanalytics.Context(
        system_instruction=(
            "You are an expert NRL analyst. "
            "The BigQuery table has EXACTLY these columns:\n"
            f"{schema_str}\n\n"
            "Rules:\n"
            "1. Answer concisely in a 'Summary' section and an 'Insights' section.\n"
            "2. After the insights, output a single line starting with 'SQL:' containing "
            "   a valid BigQuery SELECT statement that retrieves the data relevant to the "
            "   user's question. Only use column names from the schema above. Limit to 50 rows.\n"
            "3. After the SQL line, output 2-3 follow-up questions each on its own line.\n"
            "4. Do NOT describe your internal thinking, process, or query generation."
        ),
        datasource_references=geminidataanalytics.DatasourceReferences(
            bq=geminidataanalytics.BigQueryTableReferences(table_references=[bq_ref])
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

    raw = "\n".join(parts).strip()

    # ── ANCHOR TO "Summary" — discard everything before it ──
    # Thinking/reasoning always appears before the Summary heading.
    # If no Summary heading exists, fall back to the full text.
    summary_match = re.search(r"(#+\s*Summary|^Summary)", raw, re.IGNORECASE | re.MULTILINE)
    if summary_match:
        raw = raw[summary_match.start():]

    # ── Parse SQL out of the response ──
    sql = None
    sql_match = re.search(r"SQL:\s*(SELECT[\s\S]+?)(?:\n[A-Z]|\n\n|\Z)", raw, re.IGNORECASE)
    if sql_match:
        sql = sql_match.group(1).strip().rstrip(";") + ";"

    # ── Parse follow-up questions ──
    followups = [
        ln.strip().lstrip("-•123456789. ").strip()
        for ln in raw.splitlines()
        if ln.strip().endswith("?")
    ]

    # ── Build clean display text (remove SQL line and follow-up questions) ──
    display_lines = []
    for ln in raw.splitlines():
        clean = ln.strip()
        if not clean:
            continue
        if clean.lower().startswith("sql:"):
            continue
        if clean.endswith("?"):
            continue  # handled in follow-ups section
        display_lines.append(clean)

    summary = "\n\n".join(display_lines).strip()
    return {"summary": summary, "sql": sql, "followups": followups}


def run_sql(sql: str) -> pd.DataFrame | None:
    """Execute a SQL string against BigQuery and return a DataFrame."""
    try:
        bq = bigquery.Client(project=MY_PROJECT)
        df = bq.query(sql).to_dataframe()
        if df.empty:
            return None
        # Coerce numerics where possible
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="ignore")
        return df
    except Exception as e:
        st.warning(f"Chart query failed: {e}")
        return None


def render_chart(df: pd.DataFrame):
    """Auto-detect best chart type from DataFrame columns."""
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    text_cols    = [c for c in df.columns if c not in numeric_cols]

    if not numeric_cols:
        st.info("No numeric columns in the data — showing table only.")
        return

    if len(numeric_cols) >= 2:
        x_col = numeric_cols[0]
        y_col = numeric_cols[1]
        st.caption(f"Scatter: {x_col} vs {y_col}")
        st.scatter_chart(df, x=x_col, y=y_col)
    else:
        metric_col = numeric_cols[0]
        if text_cols:
            chart_df = df.set_index(text_cols[0])[[metric_col]]
        else:
            chart_df = df[[metric_col]]
        st.caption(f"Bar chart: {metric_col}")
        st.bar_chart(chart_df)


# ─────────────────────────────────────────────
# CHAT INPUT
# ─────────────────────────────────────────────
if prompt := st.chat_input("Ask anything about NRL stats…"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing NRL stats..."):
            try:
                # 1. Fetch real schema (cached after first call)
                schema_str, numeric_cols, all_cols = get_schema_string()

                # 2. Ask agent — returns summary text + SQL it generated
                result = ask_agent(prompt, schema_str)

                # 3. Run the agent's own SQL directly in BigQuery
                df = None
                if result["sql"]:
                    df = run_sql(result["sql"])

                # 4. Render chart + table
                if df is not None:
                    st.subheader("Data Results")
                    render_chart(df)
                    st.dataframe(df, use_container_width=True)
                else:
                    st.info("No chart data returned for this query.")

                # 5. Render summary
                if result["summary"]:
                    st.markdown(result["summary"])

                # 6. Render follow-ups
                if result["followups"]:
                    st.markdown("### Suggested follow up questions")
                    for q in result["followups"]:
                        st.markdown(f"- {q}")

                # 7. Save to history
                history_text = result["summary"]
                if result["followups"]:
                    fq = "\n".join(f"- {q}" for q in result["followups"])
                    history_text += f"\n\n### Suggested follow up questions\n\n{fq}"
                st.session_state.messages.append({"role": "assistant", "content": history_text})

            except Exception as e:
                st.error(f"❌ System Error: {e}")
