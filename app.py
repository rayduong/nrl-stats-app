import streamlit as st
from google.cloud import geminidataanalytics
from google.protobuf.json_format import MessageToDict
import json
import os
import pandas as pd

# --- PAGE CONFIGURATION ---
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
else:
    st.error("❌ 'GOOGLE_CREDENTIALS' not found in Secrets.")
    st.stop()

# --- SIDEBAR ---
with st.sidebar:
    st.title("🏆 NRL Agent Settings")
    if st.button("Clear Chat History"):
        st.session_state.messages = []
        st.rerun()

st.title("🏆 NRL Stats AI Agent")

if "messages" not in st.session_state:
    st.session_state.messages = []

# Display previous messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- CHAT INPUT ---
if prompt := st.chat_input("Ask about Round 1 stats..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing NRL Data..."):
            try:
                client = geminidataanalytics.DataChatServiceClient()
                
                MY_PROJECT = "nrl-2026-489302" 
                MY_LOCATION = "global"
                MY_DATASET = "player_stats" 
                MY_TABLE = "player_stats_test_2"
                
                bq_ref = geminidataanalytics.BigQueryTableReference(
                    project_id=MY_PROJECT, dataset_id=MY_DATASET, table_id=MY_TABLE
                )
                
                my_context = geminidataanalytics.Context(
                    system_instruction="You are a professional NRL analyst. Provide clean summaries. Do not include internal thoughts or thinking steps.",
                    datasource_references=geminidataanalytics.DatasourceReferences(
                        bq=geminidataanalytics.BigQueryTableReferences(table_references=[bq_ref])
                    )
                )
                
                request = geminidataanalytics.ChatRequest(
                    parent=f"projects/{MY_PROJECT}/locations/{MY_LOCATION}",
                    inline_context=my_context,
                    messages=[geminidataanalytics.Message(user_message=geminidataanalytics.UserMessage(text=prompt))]
                )
                
                # 4. STREAM & EXTRACTION
                stream = client.chat(request=request)
                final_text = ""
                table_df = None
                
                for reply in stream:
                    if hasattr(reply, 'system_message'):
                        sm = reply.system_message
                        
                        # A. CLEAN TEXT EXTRACTION
                        if hasattr(sm, 'text'):
                            # Only capture FINAL_RESPONSE (Type 3)
                            # We also filter out segments that end with a '?' to remove suggested questions
                            if "FINAL_RESPONSE" in str(sm.text.text_type) or sm.text.text_type == 3:
                                for part in sm.text.parts:
                                    if not part.strip().endswith("?"):
                                        final_text += part + "\n"

                        # B. DATA TABLE EXTRACTION (The Fix)
                        if hasattr(sm, 'data') and sm.data.result.data:
                            try:
                                rows = []
                                for row in sm.data.result.data:
                                    # Convert the Protobuf 'MapComposite' to a standard Python dict
                                    row_dict = MessageToDict(row._pb)
                                    # Extract the actual values from the 'fields' key
                                    if 'fields' in row_dict:
                                        # Flattens { 'fields': { 'player': {'string_value': 'Name'} } } 
                                        # into { 'player': 'Name' }
                                        flat_row = {}
                                        for k, v in row_dict['fields'].items():
                                            # Grab the first available value (string, number, etc)
                                            flat_row[k] = list(v.values())[0]
                                        rows.append(flat_row)
                                
                                if rows:
                                    table_df = pd.DataFrame(rows)
                            except Exception as data_err:
                                # If table parsing fails, we don't want to crash the whole app
                                print(f"Table Parse Error: {data_err}")

                # 5. DISPLAY RESULTS
                if table_df is not None and not table_df.empty:
                    st.subheader("Data Table")
                    st.dataframe(table_df, use_container_width=True)

                if final_text.strip():
                    st.markdown(final_text)
                    st.session_state.messages.append({"role": "assistant", "content": final_text})
                elif table_df is None:
                    st.warning("The agent processed the query but didn't return a final summary or table.")
                    
            except Exception as e:
                st.error(f"Main Error: {e}")
