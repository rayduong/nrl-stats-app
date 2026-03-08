import streamlit as st
from google.cloud import geminidataanalytics
from google.protobuf.json_format import MessageToDict
import json
import os
import pandas as pd

# --- PAGE CONFIG ---
st.set_page_config(page_title="NRL Stats AI", page_icon="🏆", layout="wide")

# 1. AUTHENTICATION (The "Foolproof" Version)
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
                    project_id=MY_PROJECT, dataset_id=MY_DATASET, table_id=MY_TABLE
                )

                my_context = geminidataanalytics.Context(
                    system_instruction=(
                        "You are an expert NRL analyst. "
                        "Provide concise summaries and insights for end users. "
                        "Do NOT describe your internal reasoning, SQL generation, or tools. "
                        "Only output user-facing explanations of the results."
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

                # Collect only final user-facing text and tabular data
                final_text_chunks = []
                found_data = []

                # Strings we don't want to show even if they slip through
                HIDE_KEYWORDS = [
                    "sql",
                    "SAFE_CAST",
                    "SAFE_DIVIDE",
                    "query plan",
                    "refined SQL query",
                    "I will now",
                    "I'm going to",
                    "thinking",
                    "reasoning",
                    "step",
                    "tool",
                ]

                for reply in stream:
                    # Each reply is a Message
                    # We only care about system_message because it contains
                    # the model's streamed content and data.
                    if not hasattr(reply, "system_message"):
                        continue

                    sm = reply.system_message

                    # --- TEXT: collect only user-facing parts ---
                    if getattr(sm, "text", None) is not None:
                        # sm.text.parts is a list of simple strings in Python client
                        for part in sm.text.parts:
                            if not part:
                                continue
                            # Filter any line that clearly looks like reasoning / SQL commentary
                            if any(k.lower() in part.lower() for k in HIDE_KEYWORDS):
                                continue
                            # Optional: hide automatic follow-up questions if you want
                            if part.strip().endswith("?"):
                                continue
                            final_text_chunks.append(part)

                    # --- DATA: flatten any result data into DataFrames ---
                    if (
                        getattr(sm, "data", None) is not None
                        and getattr(sm.data, "result", None) is not None
                        and sm.data.result.data
                    ):
                        try:
                            rows = []
                            for row in sm.data.result.data:
                                # Convert protobuf Row to dict
                                row_dict = MessageToDict(row._pb)
                                # Expect schema: {"fields": {"col": {"stringValue": "x"} ...}}
                                if "fields" in row_dict:
                                    flat_row = {}
                                    for col_name, val_dict in row_dict["fields"].items():
                                        # val_dict is like {"stringValue": "..."} or {"numberValue": 123}
                                        value = list(val_dict.values())[0]
                                        flat_row[col_name] = value
                                    rows.append(flat_row)
                            if rows:
                                df = pd.DataFrame(rows)

                                # Try to coerce numeric columns
                                for col in df.columns:
                                    df[col] = pd.to_numeric(df[col], errors="ignore")

                                found_data.append(df)
                        except Exception:
                            # Swallow data flattening errors, but don't break chat
                            pass

                final_text = "\n".join(final_text_chunks).strip()

                # 5. RENDER VISUALS
                if found_data:
                    for df in found_data:
                        st.subheader("Data Analysis Results")

                        # Numeric columns for charts
                        num_cols = df.select_dtypes(include=["number"]).columns.tolist()

                        if len(num_cols) >= 2:
                            # Scatter chart – use first two numeric cols
                            x_col = num_cols[0]
                            y_col = num_cols[1]
                            st.caption(f"Scatter: {x_col} vs {y_col}")
                            st.scatter_chart(df, x=x_col, y=y_col)
                        elif len(num_cols) == 1:
                            metric_col = num_cols[0]
                            st.caption(f"Bar chart of {metric_col}")
                            # Use first non-numeric column (if any) as index/labels
                            index_candidates = [c for c in df.columns if c != metric_col]
                            if index_candidates:
                                idx_col = index_candidates[0]
                                chart_df = df.set_index(idx_col)[[metric_col]]
                            else:
                                # Fallback to numeric index
                                chart_df = df[[metric_col]]
                            st.bar_chart(chart_df)

                        # Always show table
                        st.dataframe(df, use_container_width=True)

                # 6. RENDER SUMMARY
                if final_text:
                    st.markdown(final_text)
                    st.session_state.messages.append(
                        {"role": "assistant", "content": final_text}
                    )
                elif not found_data:
                    st.error(
                        "No data found. Try: 'List top 5 players by runs in Round 1.'"
                    )

            except Exception as e:
                st.error(f"❌ System Error: {e}")
