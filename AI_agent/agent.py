import os
from dotenv import load_dotenv  # <--- NEW IMPORT

# Load the .env file before anything else
load_dotenv() # <--- LOAD ENV

from dataclasses import dataclass
from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext
import asyncio
import json
import logging
from devtools import pprint
import colorlog
from httpx import AsyncClient, HTTPStatusError



log_format = (
    '%(log_color)s%(asctime)s [%(levelname)s] %(reset)s'
    '%(purple)s[%(name)s] %(reset)s%(blue)s%(message)s'
)
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(log_format))
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)

@dataclass
class Deps:
    airflow_api_base_uri: str
    airflow_api_port: int
    airflow_api_token: str

class DAGStatus(BaseModel):
    dag_id: str
    dag_display_name: str
    is_paused: bool
    next_dag_run_data_interval_start: str | None
    next_dag_run_data_interval_end: str | None
    last_dag_run_id: str
    last_dag_run_state: str
    total_dag_runs: int

airflow_agent = Agent(
    model='gemini-2.0-flash',
    system_prompt=(
        'You are an Airflow monitoring assistant.\n'
        '1. Call list_dags to discover DAGs\n'
        '2. Match the most relevant DAG\n'
        '3. Call get_dag_status to retrieve its status'
    ),
    output_type=DAGStatus,  # FIXED: result_type -> output_type
    deps_type=Deps,
    retries=2,
)

async def get_airflow_token(base_url: str, port: int, username: str, passw: str) -> str:
    """Exchanges credentials for a JWT Access Token via Airflow 3.0 Auth API. For testing only."""
    url = f"{base_url}:{port}/auth/token"
    payload = {
        "username": username,
        "password": passw
    }
    
    logger.info("Authenticating with Airflow...")
    async with AsyncClient() as client:
        try:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            token = resp.json().get("access_token")
            logger.info("Authentication successful.")
            return token
        except HTTPStatusError as e:
            logger.error(f"Auth failed: {e.response.text}")
            raise

@airflow_agent.tool
async def list_dags(ctx: RunContext[Deps]) -> str:
    logger.info('Listing DAGs...')
    url = (
        f'{ctx.deps.airflow_api_base_uri}:'
        f'{ctx.deps.airflow_api_port}/api/v2/dags'
    )

    headers = {
        'Authorization': f'Bearer {ctx.deps.airflow_api_token}'
    }

    async with AsyncClient() as client:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        dags = resp.json()['dags']
        return json.dumps([
            {'dag_id': d['dag_id'], 'dag_display_name': d['dag_display_name']}
            for d in dags
        ])

@airflow_agent.tool
async def get_dag_status(ctx: RunContext[Deps], dag_id: str) -> dict:
    logger.info(f'Fetching status for DAG: {dag_id}')
    base = f'{ctx.deps.airflow_api_base_uri}:{ctx.deps.airflow_api_port}/api/v2'
    headers = {'Authorization': f'Bearer {ctx.deps.airflow_api_token}'}

    async with AsyncClient() as client:
        dag_resp = await client.get(f'{base}/dags/{dag_id}', headers=headers)
        dag_resp.raise_for_status()

        runs_resp = await client.get(
            f'{base}/dags/{dag_id}/dagRuns',
            headers=headers,
            params={'order_by': '-logical_date', 'limit': 1},
        )
        runs_resp.raise_for_status()

        dag = dag_resp.json()
        runs = runs_resp.json()['dag_runs']
        last_run = runs[0] if runs else {}

        return {
            'dag_id': dag['dag_id'],
            'dag_display_name': dag['dag_display_name'],
            'is_paused': dag['is_paused'],
            'next_dag_run_data_interval_start': dag.get('next_dagrun_data_interval_start'),
            'next_dag_run_data_interval_end': dag.get('next_dagrun_data_interval_end'),
            'last_dag_run_id': last_run.get('dag_run_id', 'No DAG run'),
            'last_dag_run_state': last_run.get('state', 'No DAG run'),
            'total_dag_runs': len(runs),
        }

async def main():
    base_uri = 'http://localhost'
    port = 8080
    
    try:
        token = await get_airflow_token(base_uri, port, "admin", "password")
    except Exception:
        return

    # 2. Inject token into Deps
    deps = Deps(
        airflow_api_base_uri=base_uri,
        airflow_api_port=port,
        airflow_api_token=token
    )

    question = 'What is the status of the DAG for our daily payment report and also of receiver? can you give me a summary?'
    result = await airflow_agent.run(question, deps=deps)
    pprint(result.response)

if __name__ == '__main__':
    asyncio.run(main())