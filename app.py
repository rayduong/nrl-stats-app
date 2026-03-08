import streamlit as st
from google.cloud import geminidataanalytics
import json
import os

# 1. SETUP THE INTERFACE (The part users see)
st.title("🏆 NRL Stats AI Agent")
st.write("Ask me anything about player stats from the current season.")

# 2. AUTHENTICATION (How the app "logs in" to Google)
# We will pull the key from Streamlit's "Secrets" tool later
if "GOOGLE_CREDENTIALS" in st.secrets:
    creds_dict = json.loads(st.secrets["GOOGLE_CREDENTIALS"])
    with open("temp_key.json", "w") as f:
        json.dump(creds_dict, f)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "temp_key.json"

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

    # Call the BigQuery Agent
    with st.chat_message("assistant"):
        try:
            client = geminidataanalytics.DataChatServiceClient()
            # REPLACE WITH YOUR ACTUAL PROJECT/LOCATION/AGENT ID
            agent_path = client.data_agent_path("your-project", "global", "your-agent-id")
            
            # (Simplifying: In a real app, you'd manage conversation IDs, 
            # but this gets you started with a single response)
            conversation = client.create_conversation(
                parent="projects/your-project/locations/global",
                conversation=geminidataanalytics.Conversation(agents=[agent_path])
            )
            
            response = client.send_message(
                request=geminidataanalytics.SendMessageRequest(
                    name=conversation.name,
                    message=geminidataanalytics.Message(
                        text_message=geminidataanalytics.TextMessage(text=prompt)
                    )
                )
            )
            answer = response.message.text_message.text
            st.markdown(answer)
            st.session_state.messages.append({"role": "assistant", "content": answer})
        except Exception as e:
            st.error(f"Error: {e}")
