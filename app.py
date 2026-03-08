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
    st.title("🏆 NRL AI Settings")
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
if prompt := st.chat_input("E.g., What is the correlation between runs and tries in Round 1?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing Round 1 Data..."):
            try:
                client = geminidataanalytics.DataChatServiceClient()
                
                # CONFIG
                MY_PROJECT = "nrl-2026-489302" 
                MY_LOCATION = "global"
                MY_DATASET = "player_stats" 
                MY_TABLE = "player_stats_test_2"
                
                bq_ref = geminidataanalytics.BigQueryTableReference(
                    project_id=MY_PROJECT, dataset_id=MY_DATASET, table_id=MY_TABLE
                )
                
                my_context = geminidataanalytics.Context(
                    system_instruction="You are a pro NRL analyst. Output clean summaries and insights only. Do not show your internal reasoning steps.",
                    datasource_references=geminidataanalytics.DatasourceReferences(
                        bq=geminidataanalytics.BigQueryTableReferences(table_references=[bq_ref])
                    )
                )
                
                request = geminidataanalytics.ChatRequest(
                    parent=f"projects/{MY_PROJECT}/locations/{MY_LOCATION}",
                    inline_context=my_context,
                    messages=[geminidataanalytics.Message(user_message=geminidataanalytics.UserMessage(text=prompt))]
                )
                
                # 4. STREAM & MULTI-DATA EXTRACTION
                stream = client.chat(request=request)
                final_text = ""
                all_data_frames = []
                
                for reply in stream:
                    if hasattr(reply, 'system_message'):
                        sm = reply.system_message
                        
                        # A. CLEAN TEXT (Exclude all internal logs)
                        if hasattr(sm, 'text'):
                            for part in sm.text.parts:
                                # BLOCK everything that looks like thinking
                                if any(x in part for x in ["Calculating", "Formulated", "SQL", "step", "Prioritizing", "leaning"]):
                                    continue
                                if part.strip().endswith("?"):
                                    continue
                                
                                final_text += part + "\n"

                        # B. ACCUMULATE ALL DATA SETS
                        if hasattr(sm, 'data') and sm.data.result.data:
                            try:
                                rows = []
                                for row in sm.data.result.data:
                                    row_dict = MessageToDict(row._pb)
                                    if 'fields' in row_dict:
                                        flat_row = {k: list(v.values())[0] for k, v in row_dict['fields'].items()}
                                        rows.append(flat_row)
                                if rows:
                                    all_data_frames.append(pd.DataFrame(rows))
                            except:
                                pass

                # 5. DYNAMIC VISUALIZATION ENGINE
                for df in all_data_frames:
                    cols = [c.lower() for c in df.columns]
                    
                    # 1. SCATTER CHART (Correlation)
                    if ('runs' in cols or 'total_runs' in cols) and ('tries' in cols or 'tries_scored' in cols):
                        st.subheader("Correlation: Runs vs Tries")
                        # Find the exact column names (case sensitive)
                        x_col = next(c for c in df.columns if c.lower() in ['runs', 'total_runs'])
                        y_col = next(c for c in df.columns if c.lower() in ['tries', 'tries_scored'])
                        st.scatter_chart(df, x=x_col, y=y_col, color='player' if 'player' in df.columns else None)
                    
                    # 2. BAR CHART (Top Performers)
                    elif len(df) <= 15: # Likely a 'Top 10' or 'Top 15' list
                        st.subheader("Top Performers Breakdown")
                        st.bar_chart(df.set_index(df.columns[0]))
                    
                    # 3. DATA TABLE (General)
                    else:
                        with st.expander("View Full Dataset"):
                            st.dataframe(df, use_container_width=True)

                # 6. DISPLAY SUMMARY
                if final_text.strip():
                    st.markdown(final_text)
                    st.session_state.messages.append({"role": "assistant", "content": final_text})
                elif not all_data_frames:
                    st.warning("Query completed, but no charts or summary were returned. Try rephrasing.")
                    
            except Exception as e:
                st.error(f"Error: {e}")
