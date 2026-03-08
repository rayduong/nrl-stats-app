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
                        "Return three sections in this order: "
                        "'Summary', then 'Insights', then one or more suggested follow-up "
                        "questions on separate lines. "
                        "Do NOT describe your process, SQL, or tools."
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

                # Collect raw text parts and any tabular data
                text_buffer = []
                dataframes = []

                for reply in stream:
                    if not hasattr(reply, "system_message"):
                        continue
                    sm = reply.system_message

                    # TEXT
                    if getattr(sm, "text", None) is not None:
                        for part in sm.text.parts:
                            if part:
                                text_buffer.append(part)

                    # DATA
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
                                        value = list(val_dict.values())[0]
                                        flat_row[col_name] = value
                                    rows.append(flat_row)
                            if rows:
                                df = pd.DataFrame(rows)
                                # Coerce numerics where possible
                                for col in df.columns:
                                    df[col] = pd.to_numeric(df[col], errors="ignore")
                                dataframes.append(df)
                        except Exception:
                            pass

                raw_text = "\n".join(text_buffer).strip()

                # ========= TEXT POST-PROCESSING =========
                # We assume the model has already stopped exposing its thinking.
                # We now split into main body + suggested follow-ups.
                lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]

                main_lines = []
                followup_lines = []
                for ln in lines:
                    # Treat any question at the *end* as a suggested follow-up
                    if ln.endswith("?"):
                        followup_lines.append(ln)
                    else:
                        main_lines.append(ln)

                main_text = "\n\n".join(main_lines).strip()
                followup_text = "\n".join(f"- {q}" for q in followup_lines)

                # Combine for history: show main text plus a dedicated follow-up section
                if followup_lines:
                    display_text = (
                        f"{main_text}\n\n\n"
                        "### Suggested follow up questions\n\n"
                        f"{followup_text}"
                    )
                else:
                    display_text = main_text

                # ========= VISUALISATIONS =========
                if dataframes:
                    for df in dataframes:
                        st.subheader("Data Analysis Results")

                        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()

                        if not numeric_cols:
                            st.info("No numeric columns detected in returned data. Showing table only.")
                            st.dataframe(df, use_container_width=True)
                            continue

                        # Prefer 'runs' and 'tries' if present, otherwise first two numerics
                        runs_col_candidates = [c for c in numeric_cols if "run" in c.lower()]
                        tries_col_candidates = [c for c in numeric_cols if "try" in c.lower()]

                        if runs_col_candidates and tries_col_candidates:
                            x_col = runs_col_candidates[0]
                            y_col = tries_col_candidates[0]
                        elif len(numeric_cols) >= 2:
                            x_col, y_col = numeric_cols[0], numeric_cols[1]
                        else:
                            x_col, y_col = None, numeric_cols[0]

                        if x_col and y_col:
                            st.caption(f"Scatter: {x_col} vs {y_col}")
                            st.scatter_chart(df, x=x_col, y=y_col)
                        else:
                            metric_col = y_col
                            index_candidates = [c for c in df.columns if c != metric_col]
                            if index_candidates:
                                idx_col = index_candidates[0]
                                chart_df = df.set_index(idx_col)[[metric_col]]
                            else:
                                chart_df = df[[metric_col]]
                            st.caption(f"Bar chart of {metric_col}")
                            st.bar_chart(chart_df)

                        st.dataframe(df, use_container_width=True)
                else:
                    st.info(
                        "The agent returned narrative insights but no structured table for charting. "
                        "Try a more data-focused prompt such as "
                        "'Return a table of runs and tries by player for Round 1.'"
                    )

                # ========= RENDER TEXT ANSWER =========
                if display_text:
                    st.markdown(display_text)
                    st.session_state.messages.append(
                        {"role": "assistant", "content": display_text}
                    )

            except Exception as e:
                st.error(f"❌ System Error: {e}")
