import streamlit as st
from google.cloud import geminidataanalytics
import json
import os

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="NRL Stats AI", page_icon="🏆", layout="centered")

# 1. AUTHENTICATION (The "Foolproof" Secrets Version)
if "GOOGLE_CREDENTIALS" in st.secrets:
    try:
        creds_data = st.secrets["GOOGLE_CREDENTIALS"]
        # Handle both raw string and auto-parsed dictionary formats
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

# --- SIDEBAR & UI ---
with st.sidebar:
    st.title("🏆 NRL AI Settings")
    if st.button("Clear Chat History"):
        st.session_state.messages = []
        st.rerun()
    st.info("Ask about Round 1 player stats, tries, or team performances.")

st.title("🏆 NRL Stats AI Agent")
st.caption("Querying live BigQuery data for Round 1 insights.")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- CHAT INPUT & CORE LOGIC ---
if prompt := st.chat_input("E.g., Which player scored the most tries in Round 1?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        # Add a professional spinner so users know the AI is working
        with st.spinner("Analyzing BigQuery data..."):
            try:
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
                
                # 2. Define Agent "Brain" (System Instruction)
                my_context = geminidataanalytics.Context(
                    system_instruction="""You are an expert NRL data analyst. 
                    Answer player stat questions based ONLY on the provided table. 
                    If a player name has a typo, use fuzzy matching. 
                    Always provide a clear summary and interesting insights.""",
                    datasource_references=geminidataanalytics.DatasourceReferences(
                        bq=geminidataanalytics.BigQueryTableReferences(table_references=[bq_ref])
                    )
                )
                
                # 3. Build the Request
                request = geminidataanalytics.ChatRequest(
                    parent=f"projects/{MY_PROJECT}/locations/{MY_LOCATION}",
                    inline_context=my_context,
                    messages=[
                        geminidataanalytics.Message(
                            user_message=geminidataanalytics.UserMessage(text=prompt)
                        )
                    ]
                )
                
                # 4. THE STREAMING BUCKET STRATEGY
                stream = client.chat(request=request)
                answer = ""
                
                for reply in stream:
                    if hasattr(reply, 'system_message'):
                        sm = reply.system_message
                        
                        if hasattr(sm, 'text') and sm.text.parts:
                            # Convert type to string for safety across library versions
                            t_type = str(sm.text.text_type).upper()
                            
                            # Filter: If it's NOT a 'THOUGHT', it's likely part of the answer
                            if "THOUGHT" not in t_type:
                                for part in sm.text.parts:
                                    # Don't repeat the user's prompt back to them
                                    if part.strip() != prompt.strip():
                                        answer += part + "\n"

                # 5. FINAL DISPLAY
                if answer.strip():
                    # Clean up triple-newlines and show result
                    clean_answer = answer.replace("\n\n\n", "\n\n").strip()
                    st.markdown(clean_answer)
                    st.session_state.messages.append({"role": "assistant", "content": clean_answer})
                else:
                    st.warning("Data found, but no text summary generated. Try asking: 'Give me a summary of Latrell Mitchell's stats.'")
                    
            except Exception as e:
                st.error(f"❌ Error during query: {e}")
