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

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- CHAT INPUT ---
if prompt := st.chat_input("E.g., Compare top try scorers with a bar chart"):
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
                    system_instruction="You are a professional NRL analyst. Provide summaries and insights. Do not show internal thinking steps.",
                    datasource_references=geminidataanalytics.DatasourceReferences(
                        bq=geminidataanalytics.BigQueryTableReferences(table_references=[bq_ref])
                    )
                )
                
                request = geminidataanalytics.ChatRequest(
                    parent=f"projects/{MY_PROJECT}/locations/{MY_LOCATION}",
                    inline_context=my_context,
                    messages=[geminidataanalytics.Message(user_message=geminidataanalytics.UserMessage(text=prompt))]
                )
                
                # 4. STREAM & DYNAMIC EXTRACTION
                stream = client.chat(request=request)
                final_text = ""
                data_list = []
                
                for reply in stream:
                    if hasattr(reply, 'system_message'):
                        sm = reply.system_message
                        
                        # A. CLEAN TEXT EXTRACTION
                        if hasattr(sm, 'text'):
                            for part in sm.text.parts:
                                # Block "Thinking" and "Refining" segments
                                if any(x in part for x in ["Summary", "Insights", "Round 1"]):
                                    if not any(y in part for y in ["Refining", "Considering", "Analyzing"]):
                                        if not part.strip().endswith("?"):
                                            final_text += part + "\n"

                        # B. DYNAMIC DATA COLLECTION
                        if hasattr(sm, 'data') and sm.data.result.data:
                            try:
                                for row in sm.data.result.data:
                                    row_dict = MessageToDict(row._pb)
                                    if 'fields' in row_dict:
                                        flat_row = {k: list(v.values())[0] for k, v in row_dict['fields'].items()}
                                        data_list.append(flat_row)
                            except:
                                pass

                # 5. INTELLIGENT VISUALIZATION
                if data_list:
                    df = pd.DataFrame(data_list)
                    cols = df.columns.tolist()
                    
                    st.subheader("Data Visualization")
                    
                    # Logic to decide the chart type
                    if len(cols) >= 2:
                        # 1. SCATTER: If two or more numeric columns exist (e.g., runs vs metres)
                        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                        if len(numeric_cols) >= 2:
                            st.scatter_chart(df, x=numeric_cols[0], y=numeric_cols[1], color=cols[0])
                        
                        # 2. LINE: If a time-based or sequence column exists (e.g., Round)
                        elif any(x in str(cols).lower() for x in ['round', 'date', 'time']):
                            st.line_chart(df.set_index(cols[0]))
                            
                        # 3. BAR: Default for categorical stats (e.g., Player vs Tries)
                        else:
                            st.bar_chart(df.set_index(cols[0]))
                    
                    # Always show the raw table in an expander for transparency
                    with st.expander("View Raw Data Table"):
                        st.dataframe(df, use_container_width=True)

                # 6. DISPLAY FINAL TEXT
                if final_text.strip():
                    st.markdown(final_text)
                    st.session_state.messages.append({"role": "assistant", "content": final_text})
                elif not data_list:
                    st.warning("Query processed, but no summary or data was generated.")
                    
            except Exception as e:
                st.error(f"Error: {e}")
