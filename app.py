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

    # Call the BigQuery Agent
    with st.chat_message("assistant"):
        try:
            client = geminidataanalytics.DataChatServiceClient()
            
            MY_PROJECT = "nrl-2026-489302" 
            MY_AGENT = "agent_84bf3396-2e84-456b-b1bd-1d386c7f80a9"
            MY_LOCATION = "global"
            
            agent_path = client.data_agent_path(MY_PROJECT, MY_LOCATION, MY_AGENT)
            
            # 1. Create a new conversation session
            conversation = client.create_conversation(
                parent=f"projects/{MY_PROJECT}/locations/{MY_LOCATION}",
                conversation=geminidataanalytics.Conversation(agents=[agent_path])
            )
            
            # 2. Package the question using the new ChatRequest format
            request = geminidataanalytics.ChatRequest(
                parent=f"projects/{MY_PROJECT}/locations/{MY_LOCATION}",
                messages=[
                    geminidataanalytics.Message(
                        user_message=geminidataanalytics.UserMessage(text=prompt)
                    )
                ],
                conversation_reference=geminidataanalytics.ConversationReference(
                    conversation=conversation.name
                )
            )
            
            # 3. Call the new 'chat' method (This returns a stream)
            stream = client.chat(request=request)
            
            # 4. Loop through the stream and collect the text
            answer = ""
            for reply in stream:
                # Safely check if this specific chunk contains text
                if hasattr(reply.message, "text_message") and reply.message.text_message.text:
                    answer += reply.message.text_message.text + "\n\n"
            
            # Display the final combined answer
            st.markdown(answer)
            st.session_state.messages.append({"role": "assistant", "content": answer})
            
        except Exception as e:
            st.error(f"Error: {e}")
