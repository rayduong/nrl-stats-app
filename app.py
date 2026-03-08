import streamlit as st
from google.cloud import geminidataanalytics
import json
import os

# 1. SETUP THE INTERFACE (The part users see)
st.title("🏆 NRL Stats AI Agent")
st.write("Ask me anything about player stats from the current season.")

# 2. AUTHENTICATION (The "Foolproof" Version)
if "GOOGLE_CREDENTIALS" in st.secrets:
    try:
        # Get the secret
        creds_data = st.secrets["GOOGLE_CREDENTIALS"]
        
        # Check: Is it already a dictionary (processed) or a string (raw text)?
        if isinstance(creds_data, str):
            # It's a string, so we need to "load" it
            creds_dict = json.loads(creds_data)
        else:
            # It's already a dictionary/object, so we can use it directly
            # We convert it to a standard dict just to be safe
            creds_dict = dict(creds_data)
            
        # Create the temporary file for Google Cloud to use
        with open("temp_key.json", "w") as f:
            json.dump(creds_dict, f)
            
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "temp_key.json"
        
    except Exception as e:
        st.error(f"❌ There is a problem with your Google Secrets: {e}")
        st.stop()
else:
    st.error("❌ 'GOOGLE_CREDENTIALS' not found in Streamlit Secrets.")
    st.stop()

# 3. THE CHAT LOGIC
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display previous messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# User input box
if prompt := st.chat_input("E.g., Who had the most linebreaks in Round 4?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Call the BigQuery Agent directly (Bypassing the UI Agent)
    with st.chat_message("assistant"):
        try:
            client = geminidataanalytics.DataChatServiceClient()
            
            MY_PROJECT = "nrl-2026-489302" 
            MY_LOCATION = "global"
            
            # 1. Provide the exact Data Map
            MY_DATASET = "player_stats" # <-- Replace with your dataset (e.g., nrl_stats)
            MY_TABLE = "player_stats_test_2"     # <-- Replace with your table (e.g., player_data)
            
            bq_ref = geminidataanalytics.BigQueryTableReference(
                project_id=MY_PROJECT,
                dataset_id=MY_DATASET,
                table_id=MY_TABLE
            )
            
            # 2. Build the Agent's brain entirely in code
            my_context = geminidataanalytics.Context(
                system_instruction="You are an expert NRL stats analyst. Query the provided BigQuery table to answer user questions about player statistics.",
                datasource_references=geminidataanalytics.DatasourceReferences(
                    bq=geminidataanalytics.BigQueryTableReferences(
                        table_references=[bq_ref]
                    )
                )
            )
            
            # 3. Request the chat directly using inline context
            request = geminidataanalytics.ChatRequest(
                parent=f"projects/{MY_PROJECT}/locations/{MY_LOCATION}",
                inline_context=my_context,
                messages=[
                    geminidataanalytics.Message(
                        user_message=geminidataanalytics.UserMessage(text=prompt)
                    )
                ]
            )
            
            # 4. Stream the response
            stream = client.chat(request=request)
            
            answer = ""
            for reply in stream:
                if hasattr(reply.message, "text_message") and reply.message.text_message.text:
                    answer += reply.message.text_message.text
            
            st.markdown(answer)
            st.session_state.messages.append({"role": "assistant", "content": answer})
            
        except Exception as e:
            st.error(f"Error: {e}")
