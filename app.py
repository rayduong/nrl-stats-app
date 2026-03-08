import streamlit as st
from google.cloud import geminidataanalytics
import json
import os

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="NRL Stats AI", page_icon="🏆")

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
    st.title("Settings")
    if st.button("Clear Chat History"):
        st.session_state.messages = []
        st.rerun()

# --- MAIN INTERFACE ---
st.title("🏆 NRL Stats AI Agent")

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("Ask about Round 1 stats..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
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
                system_instruction="You are an expert NRL analyst. Provide summaries and insights based on the data.",
                datasource_references=geminidataanalytics.DatasourceReferences(
                    bq=geminidataanalytics.BigQueryTableReferences(table_references=[bq_ref])
                )
            )
            
            request = geminidataanalytics.ChatRequest(
                parent=f"projects/{MY_PROJECT}/locations/{MY_LOCATION}",
                inline_context=my_context,
                messages=[geminidataanalytics.Message(user_message=geminidataanalytics.UserMessage(text=prompt))]
            )
            
            # 4. STREAM & EXTRACT (The Robust Version)
            stream = client.chat(request=request)
            answer = ""
            
            for reply in stream:
                if hasattr(reply, 'system_message'):
                    sm = reply.system_message
                    
                    # A. CAPTURE THE FINAL TEXT (Look for the label 'FINAL_RESPONSE')
                    if hasattr(sm, 'text'):
                        # This checks if the type is FINAL_RESPONSE regardless of version
                        if "FINAL_RESPONSE" in str(sm.text.text_type):
                            for part in sm.text.parts:
                                answer += part + "\n"
                    
                    # B. OPTIONAL: SHOW THE SQL (Great for transparency!)
                    # if hasattr(sm, 'data') and sm.data.generated_sql:
                    #    with st.expander("Show SQL Query"):
                    #        st.code(sm.data.generated_sql, language="sql")

            # 5. DISPLAY
            if answer.strip():
                st.markdown(answer)
                st.session_state.messages.append({"role": "assistant", "content": answer})
            else:
                # C. EMERGENCY FALLBACK: If FINAL_RESPONSE fails, take any text that isn't a "THOUGHT"
                st.info("Gathering response...")
                # (You can leave this blank or add a secondary loop if needed)

        except Exception as e:
            st.error(f"Error: {e}")
