import streamlit as st
from google.cloud import geminidataanalytics
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
        with st.spinner("Querying NRL Database..."):
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
                    system_instruction="You are a professional NRL analyst. Provide clean summaries. Do not include internal thoughts.",
                    datasource_references=geminidataanalytics.DatasourceReferences(
                        bq=geminidataanalytics.BigQueryTableReferences(table_references=[bq_ref])
                    )
                )
                
                request = geminidataanalytics.ChatRequest(
                    parent=f"projects/{MY_PROJECT}/locations/{MY_LOCATION}",
                    inline_context=my_context,
                    messages=[geminidataanalytics.Message(user_message=geminidataanalytics.UserMessage(text=prompt))]
                )
                
                # 4. STREAM & SURGICAL EXTRACTION
                stream = client.chat(request=request)
                final_text = ""
                table_data = None
                
                for reply in stream:
                    if hasattr(reply, 'system_message'):
                        sm = reply.system_message
                        
                        # A. EXTRACT THE CLEAN TEXT
                        if hasattr(sm, 'text'):
                            # Type 3 is strictly the FINAL_RESPONSE summary
                            if sm.text.text_type == 3 or "FINAL_RESPONSE" in str(sm.text.text_type):
                                for part in sm.text.parts:
                                    # We skip parts that look like follow-up questions (ending in ?)
                                    if not part.strip().endswith("?"):
                                        final_text += part + "\n"

                        # B. EXTRACT THE DATA TABLE
                        if hasattr(sm, 'data') and sm.data.result.data:
                            rows = []
                            for row_data in sm.data.result.data:
                                row_dict = {}
                                for key, val in row_data.fields.items():
                                    # Extract string or number value safely
                                    row_dict[key] = val.string_value if val.string_value else val.number_value
                                rows.append(row_dict)
                            table_data = pd.DataFrame(rows)

                # 5. DISPLAY RESULTS
                if table_data is not None:
                    st.subheader("Data Results")
                    st.dataframe(table_data, use_container_width=True)

                if final_text.strip():
                    st.markdown(final_text)
                    st.session_state.messages.append({"role": "assistant", "content": final_text})
                elif table_data is None:
                    st.warning("No summary or table was generated for this query.")
                    
            except Exception as e:
                st.error(f"Error: {e}")
