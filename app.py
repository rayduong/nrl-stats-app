import streamlit as st
from google.cloud import geminidataanalytics
import json
import os

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="NRL Stats AI", page_icon="🏆")

# 1. AUTHENTICATION (The "Foolproof" Version)
if "GOOGLE_CREDENTIALS" in st.secrets:
    try:
        creds_data = st.secrets["GOOGLE_CREDENTIALS"]
        # Handle both string and dictionary formats from Streamlit Secrets
        if isinstance(creds_data, str):
            creds_dict = json.loads(creds_data)
        else:
            creds_dict = dict(creds_data)
            
        with open("temp_key.json", "w") as f:
            json.dump(creds_dict, f)
            
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "temp_key.json"
    except Exception as e:
        st.error(f"❌ Secret Key Error: {e}")
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
    st.info("Ask about player tries, run metres, or team stats from Round 1.")

# --- MAIN INTERFACE ---
st.title("🏆 NRL Stats AI Agent")
st.write("Real-time insights powered by BigQuery.")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display previous messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# User input
if prompt := st.chat_input("E.g., Who scored the most tries in Round 1?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 3. CALL THE BIGQUERY AGENT
    with st.chat_message("assistant"):
        try:
            client = geminidataanalytics.DataChatServiceClient()
            
            # --- PROJECT CONFIGURATION ---
            MY_PROJECT = "nrl-2026-489302" 
            MY_LOCATION = "global"
            MY_DATASET = "player_stats" 
            MY_TABLE = "player_stats_test_2"
            
            # Create the data reference
            bq_ref = geminidataanalytics.BigQueryTableReference(
                project_id=MY_PROJECT,
                dataset_id=MY_DATASET,
                table_id=MY_TABLE
            )
            
            # Define the agent's persona and data map
            my_context = geminidataanalytics.Context(
                system_instruction="You are an expert NRL analyst. Use the provided table to answer questions with stats and insights.",
                datasource_references=geminidataanalytics.DatasourceReferences(
                    bq=geminidataanalytics.BigQueryTableReferences(table_references=[bq_ref])
                )
            )
            
            request = geminidataanalytics.ChatRequest(
                parent=f"projects/{MY_PROJECT}/locations/{MY_LOCATION}",
                inline_context=my_context,
                messages=[geminidataanalytics.Message(user_message=geminidataanalytics.UserMessage(text=prompt))]
            )
            
            # 4. STREAM & EXTRACT RESPONSE (The "Clean" Version)
            stream = client.chat(request=request)
            full_response = ""
            
            for reply in stream:
                if hasattr(reply, 'system_message'):
                    sm = reply.system_message
                    
                    # This is the "Filter" - it checks the type of message
                    # Type 3 = FINAL_RESPONSE
                    if hasattr(sm, 'text') and sm.text.text_type == 3:
                        for part in sm.text.parts:
                            full_response += part + "\n"
            
            # If the filter finds a formal response, show it
            if full_response.strip():
                st.markdown(full_response)
                st.session_state.messages.append({"role": "assistant", "content": full_response})
            else:
                # Fallback: if no formal response exists, show the first non-thought chunk
                # This ensures the user isn't left with a blank screen if the AI skips a step
                st.warning("The agent processed the data but didn't format a final summary.")
