import streamlit as st
from google.cloud import geminidataanalytics
from google.protobuf.json_format import MessageToDict
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

# --- CHAT INPUT ---
if prompt := st.chat_input("Show me a chart of runs vs tries for Round 1"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Executing BigQuery Stats..."):
            try:
                client = geminidataanalytics.DataChatServiceClient()

                # --- PROJECT CONFIG ---
                MY_PROJECT = "nrl-2026-489302"
                MY_LOCATION = "global"
                MY_DATASET = "player_stats"
                MY_TABLE = "player_stats_test_2"

                bq_ref = geminidataanalytics.BigQueryTableReference(
                    project_id=MY_PROJECT,
                    dataset_id=MY_DATASET,
                    table_id=MY_TABLE
                )

                my_context = geminidataanalytics.Context(
                    system_instruction=(
                        "You are an expert NRL analyst. "
                        "Answer for end users only. "
                        "Do NOT describe your thinking, process, SQL, or tools. "
                        "Write a short 'Summary' section and an optional 'Insights' section."
                    ),
                    datasource_references=geminidataanalytics.DatasourceReferences(
                        bq=geminidataanalytics.BigQueryTableReferences(
                            table_references=[bq_ref]
                        )
                    ),
                )

                request = geminidataanalytics.ChatRequest(
                    parent=f"projects/{MY_PROJECT}/locations/{MY_LOCATION}",
                    inline_context=my_context,
                    messages=[
                        geminidataanalytics.Message(
                            user_message=geminidataanalytics.UserMessage(text=prompt)
                        )
                    ],
                )

                stream = client.chat(request=request)

                # Collect all raw parts, we’ll post‑filter them
                text_buffer = []
                dataframes = []

                for reply in stream:
                    if not hasattr(reply, "system_message"):
                        continue
                    sm = reply.system_message

                    # ---- TEXT COLLECTION (raw) ----
                    if getattr(sm, "text", None) is not None:
                        for part in sm.text.parts:
                            if part:
                                text_buffer.append(part)

                    # ---- DATA COLLECTION ----
                    if (
                        getattr(sm, "data", None) is not None
                        and getattr(sm.data, "result", None) is not None
                        and sm.data.result.data
                    ):
                        try:
                            rows = []
                            for row in sm.data.result.data:
                                row_dict = MessageToDict(row._pb)
                                if "fields" in row_dict:
                                    flat_row = {}
                                    for col_name, val_dict in row_dict["fields"].items():
                                        # val_dict is e.g. {"stringValue": "10"} or {"numberValue": 10}
                                        value = list(val_dict.values())[0]
                                        flat_row[col_name] = value
                                    rows.append(flat_row)
                            if rows:
                                df = pd.DataFrame(rows)
                                # Try to coerce numerics
                                for col in df.columns:
                                    df[col] = pd.to_numeric(df[col], errors="ignore")
                                dataframes.append(df)
                        except Exception:
                            pass

                raw_text = "\n".join(text_buffer)

                # ====== TEXT POST‑FILTERING TO HIDE THINKING ======

                # 1. Keep only content from the first "Summary" onward if present
                #    Anything before that is treated as system/agent thinking.
                final_text = raw_text
                summary_idx = raw_text.lower().find("summary")
                if summary_idx != -1:
                    final_text = raw_text[summary_idx:]

                # 2. Split into lines and drop meta‑commentary that still leaked
                lines = []
                DROP_KEYWORDS = [
                    "retrieved context",
                    "formulating a query",
                    "charting the results",
                    "synthesizing insights from data",
                    "i am ready to finalize my assessment",
                    "i am ready to",
                    "i will now",
                    "i'm going to",
                    "i've analyzed the data",
                    "i have analyzed the data",
                    "the following chart visualizes",
                    "i'm focusing on visualization",
                    "i'm focusing on",
                    "thinking",
                    "sql",
                    "safe_cast",
                    "safe_divide",
                ]
                for line in final_text.splitlines():
                    clean_line = line.strip()
                    if not clean_line:
                        continue
                    if any(k in clean_line.lower() for k in DROP_KEYWORDS):
                        continue
                    lines.append(clean_line)

                final_text = "\n\n".join(lines).strip()

                # ====== RENDER VISUALS ======
                if dataframes:
                    for df in dataframes:
                        st.subheader("Data Analysis Results")

                        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()

                        # If no numeric columns, still show table for debugging
                        if not numeric_cols:
                            st.info("No numeric columns detected in returned data. Showing table only.")
                            st.dataframe(df, use_container_width=True)
                            continue

                        if len(numeric_cols) >= 2:
                            x_col, y_col = numeric_cols[0], numeric_cols[1]
                            st.caption(f"Scatter: {x_col} vs {y_col}")
                            st.scatter_chart(df, x=x_col, y=y_col)
                        else:
                            metric_col = numeric_cols[0]
                            index_candidates = [c for c in df.columns if c != metric_col]
                            if index_candidates:
                                idx_col = index_candidates[0]
                                chart_df = df.set_index(idx_col)[[metric_col]]
                            else:
                                chart_df = df[[metric_col]]
                            st.caption(f"Bar chart of {metric_col}")
                            st.bar_chart(chart_df)

                        st.dataframe(df, use_container_width=True)

                # ====== RENDER TEXT ANSWER ======
                if final_text:
                    st.markdown(final_text)
                    st.session_state.messages.append(
                        {"role": "assistant", "content": final_text}
                    )
                elif not dataframes:
                    st.error(
                        "No data or summary returned. Try: 'List top 10 players by runs in Round 1.'"
                    )

            except Exception as e:
                st.error(f"❌ System Error: {e}")
