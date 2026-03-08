import streamlit as st
from google.cloud import geminidataanalytics
import json
import os

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="NRL Stats AI", page_icon="🏆", layout="centered")

# 1. AUTHENTICATION (Streamlit Secrets to Environment Variable)
if "GOOGLE_CREDENTIALS" in st.secrets:
    try:
        creds_data = st.secrets["GOOGLE_CREDENTIALS"]
        # Handle both raw string and auto-parsed dict formats
        if isinstance(creds_data, str):
            creds_dict = json.loads(creds_data)
        else:
            creds_dict = dict(creds_data)
            
        with open("temp_key.json", "w") as f:
            json.dump(creds_dict, f)
            
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "temp_key.json"
    except Exception as e:
        st.error(f"❌ Authentication Secret Error: {e}")
        st.stop()
else:
    st.error("❌ 'GOOGLE_CREDENTIALS' not found in Streamlit Secrets.")
    st.stop()

# --- SIDEBAR & UI ELEMENTS ---
with st.sidebar:
    st.title("🏆 NRL Agent Settings")
    if st.button("Clear Chat History"):
        st.session_state.messages = []
        st.rerun()
    st.markdown("""
    **Tips:**
    * Ask about specific player stats.
    * Compare players across teams.
    * Specify the Round number (e.g., 'Round 1').
    """)

st.title("🏆 NRL Stats AI Agent")
st.caption("Real-time data insights directly from BigQuery")

# Initialize session state for chat
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- CHAT INPUT & LOGIC ---
if prompt := st.chat_input("E.g., Who had the most run metres in Round 1?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            # Initialize the Google Cloud Client
            client = geminidataanalytics.DataChatServiceClient()
            
            # --- PROJECT CONFIGURATION ---
            MY_PROJECT = "nrl-2026-489302" 
            MY_LOCATION = "global"
            MY_DATASET = "player_stats" 
            MY_TABLE = "player_stats_test_2"
            
            # 1. Create Data Reference
            bq_ref = geminidataanalytics.BigQueryTableReference(
                project_id=MY_PROJECT,
                dataset_id=MY_DATASET,
                table_id=MY_TABLE
            )
            
            # 2. Build Context (The "Brain" of the Agent)
            my_context = geminidataanalytics.Context(
                system_instruction="""You are an expert NRL data analyst. 
                Use the provided BigQuery table to answer user questions. 
                Focus on providing clear summaries and interesting insights. 
                If the user makes a typo in a player name, use fuzzy matching (LIKE or LOWER).""",
                datasource_references=geminidataanalytics.DatasourceReferences(
                    bq=geminidataanalytics.BigQueryTableReferences(table_references=[bq_ref])
                )
            )
            
            # 3. Formulate the Chat Request
            request = geminidataanalytics.ChatRequest(
                parent=f"projects/{MY_PROJECT}/locations/{MY_LOCATION}",
                inline_context=my_context,
                messages=[
                    geminidataanalytics.Message(
                        user_message=geminidataanalytics.UserMessage(text=prompt)
                    )
                ]
            )
            
            # 4. Stream and Filter the Response
            stream = client.chat(request=request)
            full_response = ""
            
            # We iterate through the stream and only collect 'FINAL_RESPONSE' (Type 3)
            for reply in stream:
                if hasattr(reply, 'system_message'):
                    sm = reply.system_message
                    # Check for text content and ensure it's the final answer, not internal 'thoughts'
                    if hasattr(sm, 'text') and sm.text.text_type == 3: 
                        for part in sm.text.parts:
                            full_response += part + "\n"

            # 5. Display the cleaned output
            if full_response.strip():
                st.markdown(full_response)
                st.session_state.messages.append({"role": "assistant", "content": full_response})
            else:
                st.warning("The agent generated data but no text summary. Try asking: 'Give me a summary of who scored the most tries in Round 1.'")
                
        except Exception as e:
            st.error(f"An error occurred: {e}")
