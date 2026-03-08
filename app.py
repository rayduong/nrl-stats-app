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
                
                # --- PROJECT CONFIG (Targeting your US-Central1 data) ---
                MY_PROJECT = "nrl-2026-489302" 
                MY_LOCATION = "global"
                MY_DATASET = "player_stats" 
                MY_TABLE = "player_stats_test_2"
                
                bq_ref = geminidataanalytics.BigQueryTableReference(
                    project_id=MY_PROJECT, dataset_id=MY_DATASET, table_id=MY_TABLE
                )
                
                my_context = geminidataanalytics.Context(
                    system_instruction="Expert NRL analyst. Provide summaries and insights. Do not show internal thinking steps.",
                    datasource_references=geminidataanalytics.DatasourceReferences(
                        bq=geminidataanalytics.BigQueryTableReferences(table_references=[bq_ref])
                    )
                )
                
                request = geminidataanalytics.ChatRequest(
                    parent=f"projects/{MY_PROJECT}/locations/{MY_LOCATION}",
                    inline_context=my_context,
                    messages=[geminidataanalytics.Message(user_message=geminidataanalytics.UserMessage(text=prompt))]
                )
                
                # 4. AGGRESSIVE EXTRACTION
                stream = client.chat(request=request)
                final_text = ""
                found_data = []
                
                # Filter out the specific thinking strings from your screenshots
                THOUGHT_KEYWORDS = ["Retrieved context", "Querying", "Generating", "Evaluating", "Formulate", "Thinking", "Analyzing", "Refining"]

                for reply in stream:
                    if hasattr(reply, 'system_message'):
                        sm = reply.system_message
                        
                        # A. TEXT: Collect everything NOT in the 'Thought' list
                        if hasattr(sm, 'text'):
                            for part in sm.text.parts:
                                if not any(word in part for word in THOUGHT_KEYWORDS):
                                    if not part.strip().endswith("?"): # Hide follow-up questions
                                        final_text += part + "\n"

                        # B. DATA: Collect and flatten ALL datasets
                        if hasattr(sm, 'data') and sm.data.result.data:
                            try:
                                rows = []
                                for row in sm.data.result.data:
                                    # Convert Google's complex object to a standard Python list
                                    row_dict = MessageToDict(row._pb)
                                    if 'fields' in row_dict:
                                        flat_row = {k: list(v.values())[0] for k, v in row_dict['fields'].items()}
                                        rows.append(flat_row)
                                if rows:
                                    found_data.append(pd.DataFrame(rows))
                            except:
                                pass

                # 5. RENDER VISUALS (Automatic chart detection)
                if found_data:
                    for df in found_data:
                        st.subheader("Data Analysis Results")
                        # Get only numeric columns for charts
                        num_cols = df.select_dtypes(include=['number']).columns.tolist()
                        
                        if len(num_cols) >= 2:
                            # Correlation query? Show Scatter Plot
                            st.scatter_chart(df, x=num_cols[0], y=num_cols[1], color=df.columns[0])
                        elif len(num_cols) == 1:
                            # Single metric? Show Bar Chart
                            st.bar_chart(df.set_index(df.columns[0]))
                        
                        # Always show the table for full detail
                        st.dataframe(df, use_container_width=True)

                # 6. RENDER SUMMARY
                if final_text.strip():
                    st.markdown(final_text)
                    st.session_state.messages.append({"role": "assistant", "content": final_text})
                elif not found_data:
                    st.error("No data found. Try: 'List top 5 players by runs in Round 1.'")
                    
            except Exception as e:
                st.error(f"❌ System Error: {e}")
