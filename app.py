import streamlit as st
import asyncio
import os
from dotenv import load_dotenv


from AI_agent.agent import airflow_agent, Deps, DAGStatus, get_airflow_token

load_dotenv()

st.set_page_config(page_title="Airflow 3.0 Agent", layout="wide")
st.title("🤖 Airflow 3.0 Agent")
st.markdown("Ask questions about your DAGs, and the AI will check the live API.")

with st.sidebar:
    st.header("🔌 Connection Config")
    # Use .env values as defaults, but allow user override in UI
    base_uri = st.text_input("API URL", value=os.getenv("AIRFLOW_API_BASE_URI", "http://localhost"))
    port = st.number_input("Port", value=int(os.getenv("AIRFLOW_API_PORT", 8080)))
    user = st.text_input("Username", value=os.getenv("AIRFLOW_API_USER", "admin"))
    password = st.text_input("Password", value=os.getenv("AIRFLOW_API_PASS", "password"), type="password")

def run_async(coroutine):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coroutine)
    finally:
        loop.close()
user_request = st.chat_input("Ex: What is the status of the payment report DAG?")

if user_request:
    with st.chat_message("user"):
        st.write(user_request)

    with st.chat_message("assistant"):
        status_container = st.status("Thinking...", expanded=True)
        
        try:
            status_container.write("🔐 Authenticating with Airflow 3.0...")
            token = run_async(get_airflow_token(base_uri, port, user, password))
            status_container.write("✅ Authentication successful!")

            deps = Deps(
                airflow_api_base_uri=base_uri,
                airflow_api_port=port,
                airflow_api_token=token  # <--- Passing the TOKEN, not password
            )

            status_container.write("🧠 Agent is analyzing DAGs...")
            result = run_async(airflow_agent.run(user_request, deps=deps))
            
            status_container.update(label="Complete!", state="complete", expanded=False)

           
            if isinstance(result.response, DAGStatus):
                st.success(f"Found DAG: **{result.response.dag_display_name}**")
                
                col1, col2, col3 = st.columns(3)
                col1.metric("State", result.response.last_dag_run_state)
                col2.metric("Paused?", "Yes" if result.response.is_paused else "No")
                col3.metric("Total Runs", result.response.total_dag_runs)
                
                with st.expander("See raw details"):
                    st.json(result.response.model_dump())
            else:
                st.write(result.response)

        except Exception as e:
            status_container.update(label="Error", state="error")
            st.error(f"An error occurred: {e}")