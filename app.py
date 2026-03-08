import streamlit as st
from google.cloud import geminidataanalytics
from google.cloud import bigquery
import json
import os
import re
import pandas as pd
import plotly.express as px

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

# --- NRL TEAM COLORS ---
NRL_TEAM_COLORS = {
    "Broncos":            "#E3001B",
    "Brisbane Broncos":   "#E3001B",
    "Raiders":            "#6ABD45",
    "Canberra Raiders":   "#6ABD45",
    "Bulldogs":           "#003F9D",
    "Canterbury Bulldogs":"#003F9D",
    "Canterbury-Bankstown Bulldogs": "#003F9D",
    "Sharks":             "#00B2F5",
    "Cronulla Sharks":    "#00B2F5",
    "Cronulla-Sutherland Sharks": "#00B2F5",
    "Titans":             "#009ACD",
    "Gold Coast Titans":  "#009ACD",
    "Sea Eagles":         "#711B1F",
    "Manly Sea Eagles":   "#711B1F",
    "Manly-Warringah Sea Eagles": "#711B1F",
    "Storm":              "#4B206E",
    "Melbourne Storm":    "#4B206E",
    "Knights":            "#003087",
    "Newcastle Knights":  "#003087",
    "Warriors":           "#808285",
    "New Zealand Warriors": "#808285",
    "Cowboys":            "#005BAC",
    "North Queensland Cowboys": "#005BAC",
    "Eels":               "#004B8D",
    "Parramatta Eels":    "#004B8D",
    "Panthers":           "#231F20",
    "Penrith Panthers":   "#231F20",
    "Rabbitohs":          "#008751",
    "South Sydney Rabbitohs": "#008751",
    "Dragons":            "#EE3124",
    "St George Illawarra Dragons": "#EE3124",
    "Roosters":           "#003A74",
    "Sydney Roosters":    "#003A74",
    "Tigers":             "#FF7300",
    "Wests Tigers":       "#FF7300",
    "Dolphins":           "#EC1C24",
}

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
    """Fetches real column names and types from BigQuery."""
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
    """Calls Gemini Data Analytics agent. Returns summary, sql, followups."""
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
            "   user's question. Always include the team/club column in the SELECT if it exists. "
            "   Only use column names from the schema above. Limit to 50 rows.\n"
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

    # Discard everything before "Summary" — that's all internal thinking
    summary_match = re.search(r"(#+\s*Summary|^Summary)", raw, re.IGNORECASE | re.MULTILINE)
    if summary_match:
        raw = raw[summary_match.start():]

    # Parse SQL
    sql = None
    sql_match = re.search(r"SQL:\s*(SELECT[\s\S]+?)(?:\n[A-Z]|\n\n|\Z)", raw, re.IGNORECASE)
    if sql_match:
        sql = sql_match.group(1).strip().rstrip(";") + ";"

    # Parse follow-ups
    followups = [
        ln.strip().lstrip("-•123456789. ").strip()
        for ln in raw.splitlines()
        if ln.strip().endswith("?")
    ]

    # Clean display text
    display_lines = []
    for ln in raw.splitlines():
        clean = ln.strip()
        if not clean:
            continue
        if clean.lower().startswith("sql:"):
            continue
        if clean.endswith("?"):
            continue
        display_lines.append(clean)

    summary = "\n\n".join(display_lines).strip()
    return {"summary": summary, "sql": sql, "followups": followups}


def run_sql(sql: str) -> pd.DataFrame | None:
    """Execute SQL against BigQuery and return a DataFrame."""
    try:
        bq = bigquery.Client(project=MY_PROJECT)
        df = bq.query(sql).to_dataframe()
        if df.empty:
            return None
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="ignore")
        return df
    except Exception as e:
        st.warning(f"Chart query failed: {e}")
        return None


def find_team_column(df: pd.DataFrame) -> str | None:
    """Detect which column contains team/club names."""
    team_keywords = ["team", "club", "squad", "franchise"]
    for col in df.columns:
        if any(kw in col.lower() for kw in team_keywords):
            return col
    return None


def render_chart(df: pd.DataFrame):
    """Render Plotly chart with NRL team colors where possible."""
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    text_cols    = [c for c in df.columns if c not in numeric_cols]
    team_col     = find_team_column(df)

    if not numeric_cols:
        st.info("No numeric columns in the data — showing table only.")
        return

    # Build color map from whatever team values exist in the data
    color_map = {}
    if team_col:
        for team_val in df[team_col].unique():
            # Try exact match first, then partial match
            if team_val in NRL_TEAM_COLORS:
                color_map[team_val] = NRL_TEAM_COLORS[team_val]
            else:
                for key, hex_color in NRL_TEAM_COLORS.items():
                    if key.lower() in str(team_val).lower() or str(team_val).lower() in key.lower():
                        color_map[team_val] = hex_color
                        break

    if len(numeric_cols) >= 2:
        x_col = numeric_cols[0]
        y_col = numeric_cols[1]

        # Determine hover/label column (player name preferred)
        hover_col = next(
            (c for c in text_cols if "player" in c.lower() or "name" in c.lower()),
            text_cols[0] if text_cols else None,
        )

        fig = px.scatter(
            df,
            x=x_col,
            y=y_col,
            color=team_col if team_col else (hover_col if hover_col else None),
            color_discrete_map=color_map if color_map else None,
            hover_name=hover_col,
            labels={x_col: x_col.replace("_", " ").title(),
                    y_col: y_col.replace("_", " ").title()},
            title=f"{x_col.replace('_', ' ').title()} vs {y_col.replace('_', ' ').title()}",
        )
        fig.update_traces(marker=dict(size=10, opacity=0.85))
        fig.update_layout(legend_title_text="Team")
        st.plotly_chart(fig, use_container_width=True, theme=None)

    else:
        metric_col = numeric_cols[0]
        label_col  = text_cols[0] if text_cols else None

        if label_col:
            # Add team color column for bar chart
            if team_col and color_map:
                df["_color"] = df[team_col].map(color_map).fillna("#AAAAAA")
                fig = px.bar(
                    df,
                    x=label_col,
                    y=metric_col,
                    color=team_col,
                    color_discrete_map=color_map,
                    labels={metric_col: metric_col.replace("_", " ").title(),
                            label_col: label_col.replace("_", " ").title()},
                    title=metric_col.replace("_", " ").title(),
                )
            else:
                fig = px.bar(
                    df,
                    x=label_col,
                    y=metric_col,
                    labels={metric_col: metric_col.replace("_", " ").title(),
                            label_col: label_col.replace("_", " ").title()},
                    title=metric_col.replace("_", " ").title(),
                )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True, theme=None)
        else:
            fig = px.bar(df, y=metric_col, title=metric_col.replace("_", " ").title())
            st.plotly_chart(fig, use_container_width=True, theme=None)


# ─────────────────────────────────────────────
# CHAT INPUT
# ─────────────────────────────────────────────
if prompt := st.chat_input("Ask anything about NRL stats…"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("G'day mate, have a 🍺 whilst I check on..."):
            try:
                # 1. Fetch real schema (cached after first call)
                schema_str, numeric_cols, all_cols = get_schema_string()

                # 2. Ask agent
                result = ask_agent(prompt, schema_str)

                # 3. Run the agent's SQL directly in BigQuery
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
