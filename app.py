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
        st.error(f"❌ Auth Error: {e}")
        st.stop()

# --- MAIN UI ---
st.title("🏆 NRL Stats AI Agent")

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("Show me a chart for top runners in Round 1"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Executing BigQuery Stats..."):
            try:
                client = geminidataanalytics.DataChatServiceClient()
                
                # CONFIG (Matches your US-Central1 dataset from Screenshot 1)
                MY_PROJECT = "nrl-2026-489302" 
                MY_LOCATION = "global"
                MY_DATASET = "player_stats" 
                MY_TABLE = "player_stats_test_2"
                
                bq_ref = geminidataanalytics.BigQueryTableReference(
                    project_id=MY_PROJECT, dataset_id=MY_DATASET, table_id=MY_TABLE
                )
                
                my_context = geminidataanalytics.Context(
                    system_instruction="Expert NRL analyst. Clean insights only. No internal thoughts.",
                    datasource_references=geminidataanalytics.DatasourceReferences(
                        bq=geminidataanalytics.BigQueryTableReferences(table_references=[bq_ref])
                    )
                )
                
                request = geminidataanalytics.ChatRequest(
                    parent=f"projects/{MY_PROJECT}/locations/{MY_LOCATION}",
                    inline_context=my_context,
                    messages=[geminidataanalytics.Message(user_message=geminidataanalytics.UserMessage(text=prompt))]
                )
                
                # 4. REFINED STREAM EXTRACTION
                stream = client.chat(request=request)
                final_text = ""
                all_dfs = []
                
                # Keywords that identify the "Thinking" logs from your screenshot
                JUNK = ["Retrieved context", "Querying", "Generating", "Evaluating", "Formulate", "Thinking", "Analyzing"]

                for reply in stream:
                    if hasattr(reply, 'system_message'):
                        sm = reply.system_message
                        
                        # A. TEXT FILTERING
                        if hasattr(sm, 'text'):
                            for part in sm.text.parts:
                                # BLOCK logic: If it looks like a thinking step, skip it
                                if any(word in part for word in JUNK):
                                    continue
                                # BLOCK logic: Skip follow-up question suggestions
                                if part.strip().endswith("?"):
                                    continue
                                final_text += part + "\n"

                        # B. DATA CAPTURE
                        if hasattr(sm, 'data') and sm.data.result.data:
                            try:
                                rows = []
                                for row in sm.data.result.data:
                                    row_dict = MessageToDict(row._pb)
                                    if 'fields' in row_dict:
                                        flat_row = {k: list(v.values())[0] for k, v in row_dict['fields'].items()}
                                        rows.append(flat_row)
                                if rows:
                                    all_dfs.append(pd.DataFrame(rows))
                            except:
                                pass

                # 5. DYNAMIC VISUALIZATION DISPLAY
                if all_dfs:
                    for df in all_dfs:
                        st.subheader("Data Visualization")
                        num_cols = df.select_dtypes(include=['number']).columns.tolist()
                        
                        # Use Scatter for Correlation (like Runs vs Metres)
                        if len(num_cols) >= 2:
                            st.scatter_chart(df, x=num_cols[0], y=num_cols[1], color=df.columns[0])
                        # Use Bar for everything else
                        else:
                            st.bar_chart(df.set_index(df.columns[0]))
                        
                        with st.expander("View Raw Data"):
                            st.dataframe(df, use_container_width=True)

                # 6. DISPLAY SUMMARY
                if final_text.strip():
                    st.markdown(final_text)
                    st.session_state.messages.append({"role": "assistant", "content": final_text})
                elif not all_dfs:
                    st.warning("No answer or chart could be generated. Try: 'Compare runs and tries for Round 1 players in a table'.")
                    
            except Exception as e:
                st.error(f"❌ Error: {e}")
