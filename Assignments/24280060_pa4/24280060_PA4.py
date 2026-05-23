# Databricks notebook source
# MAGIC %pip install langgraph
# MAGIC %pip install langchain
# MAGIC %pip install pygame
# MAGIC

# COMMAND ----------

import os
from dotenv import load_dotenv
from databricks_langchain import ChatDatabricks
from langchain_core.messages import SystemMessage, HumanMessage as LCHumanMessage


# COMMAND ----------


os.environ.pop("DATABRICKS_HOST", None)
os.environ.pop("DATABRICKS_TOKEN", None)
os.environ.pop("DATABRICKS_AUTH_TYPE", None)
os.environ.pop("DATABRICKS_METADATA_SERVICE_URL", None)
os.environ.pop("DATABRICKS_SERVERLESS_COMPUTE_ID", None)

# COMMAND ----------


load_dotenv()

print("DATABRICKS_HOST:", "SET" if os.getenv("DATABRICKS_HOST") else "NOT SET")
print("DATABRICKS_TOKEN:", "SET" if os.getenv("DATABRICKS_TOKEN") else "NOT SET")

os.environ["DATABRICKS_AUTH_TYPE"] = "pat"

print("HOST:", os.getenv("DATABRICKS_HOST"))
print("TOKEN:", "SET" if os.getenv("DATABRICKS_TOKEN") else "NOT SET")
print("AUTH:", os.getenv("DATABRICKS_AUTH_TYPE"))

llm = ChatDatabricks(
    endpoint="databricks-qwen3-next-80b-a3b-instruct", # replaced this due to rate limits "databricks-meta-llama-3.1-405b-instruct",
    temperature=0.3,
    max_tokens=4096,
)

def call_llm(system_prompt: str, user_prompt: str, max_tokens: int = 4096) -> str:
    """Call Mosaic AI via ChatDatabricks and return the text reply."""
    messages = [
        SystemMessage(content=system_prompt),
        LCHumanMessage(content=user_prompt),
    ]
    response = llm.invoke(messages)
    return response.content

# COMMAND ----------

import os
import mlflow
import subprocess
from typing import Callable
from dataclasses import dataclass
from typing import TypedDict, Annotated, List
from langchain_core.messages import BaseMessage, AIMessage, HumanMessage
from langgraph.graph.message import add_messages
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver

# COMMAND ----------

# MAGIC %md
# MAGIC ##Task 1: Initializing System Memory and State Management [10 marks]

# COMMAND ----------

def add_strings(str1: List[str], str2: List[str]) -> List[str]:
    """list append reducer just like add_messages but for strings"""
    if str1 is None:
        str1 = []
    if str2 is None:
        return str1
    return str1 + str2


# COMMAND ----------

class GameState(TypedDict):
    director_messages: Annotated[List[BaseMessage], add_messages]
    architect_messages: Annotated[List[BaseMessage], add_messages]
    engineer_code: Annotated[List[str], add_strings]
    qa_feedback: Annotated[List[str], add_strings]
    current_actor: str
    iteration: int 
    iteration_score: Annotated[List[float], add_strings]
    file_saved: bool  


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Implement Agent Nodes [40 marks]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Subtask 2.1: Director and Architect Nodes

# COMMAND ----------

def director_node(state: GameState):
    iteration = state.get("iteration", 0)

    if iteration == 0:
        director_msg = input(
            "Awaiting Director Prompt: "
        ).strip()
        if not director_msg:
            director_msg = (
                "Build a Chrome-style Dino Runner game in Python using pygame. "
                "Include a jumping dinosaur, ground cacti, flying pterodactyls, "
                "accurate physics, duck mechanics, and a high-score tracker."
            )
        print(f"Director Goal recorded: {director_msg[:100]}...")
        return {
            "director_messages": [HumanMessage(content=director_msg)],
            "current_actor": "director",
        }
    else:
        print("Director Using existing goal from state.")
        return {"current_actor": "director"}


def architect_node(state: GameState):
    iteration = state.get("iteration", 0)
    print(f"\n========== ITERATION {iteration} ==========")

    director_messages = state.get("director_messages", [])
    director_goal = (
        director_messages[0].content
        if director_messages
        else "Build a Dino Runner game using pygame."
    )

    qa_feedback_list = state.get("qa_feedback", [])
    qa_context = ""
    if qa_feedback_list:
        qa_context = f"\n\n\n----Previous QA Feedback to address:\n{qa_feedback_list[-1]}"

    system_prompt = (
        "You are a senior software architect specialising in Python game development. "
        "Produce a detailed, structured technical design document for the requested game. "
        "Cover: class structure, game loop, physics model, asset strategy, input handling, "
        "scoring system, and any edge-cases. Be precise and implementable."
    )
    user_prompt = (
        f"Director Goal:\n{director_goal}"
        f"{qa_context}\n\n"
        "Produce the full technical design document now."
    )

    print("[Architect] Generating design document via LLM …")
    design_doc = call_llm(system_prompt, user_prompt, max_tokens=2048)
    print(f"Architect - Design ready ({len(design_doc)} chars).")

    return {
        "architect_messages": [AIMessage(content=design_doc)],
        "current_actor": "architect",
        "iteration": iteration + 1,
        "file_saved": False,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Subtask 2.2: Engineer Node

# COMMAND ----------

import re

def _extract_code(raw: str) -> str:
    """ """
    match = re.search(r"```(?:python)?\n(.*?)```", raw, re.DOTALL)
    return match.group(1).strip() if match else raw.strip()


def engineer_node(state: GameState) -> dict:
    architect_messages = state.get("architect_messages", [])
    architect_message   = (
        architect_messages[-1].content
        if architect_messages
        else "No architect design available."
    )

    prev_code_list= state.get("engineer_code", [])
    prev_code= prev_code_list[-1] if prev_code_list else None

    qa_messages_list= state.get("qa_feedback", [])
    qa_messages= qa_messages_list[-1] if qa_messages_list else None

    refinement_context = ""
    if prev_code and qa_messages:
        refinement_context = (
            f"\n\n\n----PREVIOUS CODE (iteration {len(prev_code_list)})----\n"
            f"{prev_code}\n"
            f"\n-----QA FEEDBACK for refinement------\n{qa_messages}\n"
            "Please fix all. the issues reported above and fix the code"
        )
## llm prompt(for help)
    system_prompt = (
        "You are a senior Python game developer. "
        "Write complete, runnable pygame code that implements the design document below. "
        "Requirements:\n"
        "  • The game must run headlessly when DISPLAY is not set (use pygame.display.set_mode with pygame.NOFRAME if needed for CI, but keep full display for normal run).\n"
        "  • Include: dinosaur jump + duck, ground cacti, flying pterodactyls, score, high-score.\n"
        "  • Output ONLY the raw Python code — no markdown, no explanation."
    )
    user_prompt = (
        f"\n\nArchitecture Design:\n{architect_message}"
        f"{refinement_context}"
    )

    print("Engineer- Generating game code using LLM")
    raw_output = call_llm(system_prompt, user_prompt, max_tokens=4096)
    clean_code = _extract_code(raw_output)
    print(f"\n\nEngineer- Code ready ({len(clean_code)} chars).")

    return {
        "engineer_code": [clean_code],
        "current_actor": "engineer",
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Subtask 2.3: File I/O and Execution Nodes

# COMMAND ----------

def file_writer(state: GameState):

    code_list = state.get("engineer_code", [])
    code = code_list[-1] if code_list else ""    

    if not code:
        print("No code to save- File writer")
        return {"file_saved": False}

    filepath = "dino_runner.py"
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(code)

    print(code[:500])
    print("\n[File saved: dino_runner.py]")

    return {"file_saved": True, "current_actor": "file_writer"}


def run_code(state: GameState):
    print("\n===== CODE EXECUTION =====")
    
    if not state.get("file_saved", False):
        print("file not saved so skipping execution")
        return {"qa_feedback": ["EXECUTION SKIPPEd- File was not saved."], "current_actor": "run_code"}
    
    choice = input("Run the generated game? (y/n): ").strip().lower()
    
    if choice != "y":
        print("Execution skipped by user.")
        run_output = "EXECUTION HAS BEENSKIPPED BY THE USER"
    else:
        print("RunCode-Launching dino_runner.py")
        try:
            result = subprocess.run(
                ["python", "dino_runner.py"],
                capture_output=True,
                text=True,
                timeout=30, 
            )
            run_output = (
                f"STDOUT:\n{result.stdout}\n"
                f"STDERR:\n{result.stderr}\n"
                f"Return code:{result.returncode}"
            )
        except subprocess.TimeoutExpired:
            run_output = "PROCESS TIMED OUT"
        except Exception as exc:
            run_output = f"EXECUTION FAILED - MSG: {exc}"

        print(run_output[:300])

    return {
        "qa_feedback": [f"RUN_OUTPUT-\n{run_output}"],
        "current_actor": "run_code",
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Subtask 2.4: QA and Scorer Nodes

# COMMAND ----------


def qa_node(state: GameState):
    
    qa_feedback_list = state.get("qa_feedback", [])
    run_output = qa_feedback_list[-1] if qa_feedback_list else "NO EXECUTION OUTPUT"

    architect_msgs = state.get("architect_messages", [])
    design_requirements = (
        architect_msgs[-1].content if architect_msgs else "NO DESIGN REQUIREMENTS BY ARCHITECT"
    )

    code_list = state.get("engineer_code", [])
    engineer_code = code_list[-1] if code_list else "NO CODE GENERATED BY ENGINEER"

    system_prompt = (
        "You are an expert QA engineer. Analyse the provided Python game code "
        "against the design requirements and the execution output. "
        "Report:1:syntax errors,2:runtime errors,3:missing features, "
        "4:logic bugs,5:recommended fixes. Be specific and actionable."
    )
    user_prompt = (
        f"----DESIGN REQUIREMENTS----\n{design_requirements[:2000]}\n\n"
        f"----ENGINEER CODE----\n{engineer_code[:3000]}\n\n"
        f"----EXECUTION OUTPUT----\n{run_output[:1000]}"
    )

    print("QA NODE ANALYSING THE CODE EXECUTION USING LLM")
    qa_analysis = call_llm(system_prompt, user_prompt, max_tokens=1500)

    return {
        "qa_feedback": [qa_analysis],
        "current_actor": "qa",
    }


def score_node(state: GameState):
    qa_feedback_list = state.get("qa_feedback", [])
    qa_feedback = qa_feedback_list[-1] if qa_feedback_list else "NO QA FEEDBACK."

    system_prompt = (
        "You are a code quality assessor. Given a QA report about a Python game, "
        "assign a single integer score from 1 (totally broken) to 10 (perfect). "
        "Reply with ONLY the integer-no other text."
    )
    user_prompt = f"QA Report:\n{qa_feedback}"

    print("COMPUTING QUALITY SCORE USING LLM")
    raw_score = call_llm(system_prompt, user_prompt, max_tokens=10).strip()
    try:
        score = float(re.search(r"\d+(\.\d+)?", raw_score).group())
        score = max(1.0, min(10.0, score))
    except Exception:
        score = 5.0

    print(f"Score for iteration {state.get('iteration', '?')}: {score}/10")

    return {
        "iteration_score": [score],
        "current_actor": "scorer",
    }

# COMMAND ----------

def should_continue(state: GameState) -> str:
    # Human-in-the-Loop routing after the Scorer.
    scores = state.get("iteration_score", [])
    latest_score = scores[-1] if scores else 0
    iteration    = state.get("iteration", 0)

    print("\n" + "="*50)
    print(f"----Current score----: {latest_score}/10")
    print(f"----Iteration--------: {iteration}")
    print("="*50)

    choice = input("Do you want to refine the code? (y/n): ").strip().lower()
    if choice == "y":
        print("Routing back to Engineer for refinement.")
        return "engineer"
    else:
        print("Workflow complete-routing ending")
        return "end"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Create Graph Structure [20 marks]

# COMMAND ----------

builder = StateGraph(GameState)
builder.add_node("director", director_node)
builder.add_node("architect", architect_node)
builder.add_node("engineer", engineer_node)
builder.add_node("file_writer", file_writer)
builder.add_node("run_code", run_code)
builder.add_node("qa", qa_node)
builder.add_node("scorer",score_node)

builder.add_edge(START,"director")
builder.add_edge("director","architect")
builder.add_edge("architect","engineer")
builder.add_edge("engineer","file_writer")
builder.add_edge("file_writer","run_code")
builder.add_edge("run_code","qa")
builder.add_edge("qa","scorer")

builder.add_conditional_edges("scorer",
    should_continue,
    {
        "engineer": "engineer",
        "end": END,
    },
)

memory= MemorySaver()
app = builder.compile(checkpointer=memory)

print("GRAPH COMPILED")
print("NODES: ", list(builder.nodes.keys()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: System Invocation [10 marks]

# COMMAND ----------

config = {"configurable": {"thread_id": "dino_runner_session_1"}}

initial_state: GameState = {
    "director_messages":[],
    "architect_messages":[],
    "engineer_code":[],
    "qa_feedback":[],
    "current_actor": "",
    "iteration":0,
    "iteration_score":[],
    "file_saved":False,
}

print("----STARTING WORKFLOW----")

for event in app.stream(initial_state, config=config, stream_mode="updates"):
    for node, value in event.items():
        print(f"\n===== {node.upper()} =====")
        if isinstance(value, dict):
            for k, v in value.items():
                if isinstance(v, list) and v:
                    preview = str(v[-1])[:120].replace("\n", " ")
                    print(f"  {k} (latest): {preview} …")
                elif isinstance(v, str) and len(v) > 100:
                    print(f"  {k}: {v[:120]} …")
                else:
                    print(f"  {k}: {v}")

print("\n--- WORKFLOW COMPLETE ---")

# COMMAND ----------

# MAGIC %md
# MAGIC Build a Chrome-style Dino Runner game using Python and pygame with the following specifications:
# MAGIC
# MAGIC VISUALS & COLORS:
# MAGIC - Sky background: light blue (#87CEEB) that darkens to purple (#2C1654) at night mode after score 500
# MAGIC - Ground: solid brown bar (#8B4513) at bottom with green grass strip (#228B22) on top
# MAGIC - Dinosaur: green (#2ECC71) rectangle body with darker green (#27AE60) legs, white eye (#FFFFFF) with black pupil, gray duck shape when ducking
# MAGIC - Cacti: dark green (#1A5C1A) with multiple arms, thick trunks, arranged in groups of 1 to 3
# MAGIC - Pterodactyls: orange (#E67E22) bird shape with two wing positions (flapping animation), flies at 3 different heights
# MAGIC - Score text: bold black font top right corner with golden (#FFD700) high score beside it
# MAGIC - Clouds: white (#FFFFFF) slow-moving puffs in the background
# MAGIC - Game Over screen: red GAME OVER text centered, with PRESS SPACE TO RESTART below it
# MAGIC
# MAGIC GAMEPLAY:
# MAGIC - Dinosaur jumps with SPACE or UP arrow, double jump allowed
# MAGIC - Dinosaur ducks with DOWN arrow, hitbox shrinks when ducking
# MAGIC - Gravity pulls dinosaur down realistically with acceleration
# MAGIC - Game speed increases every 100 points
# MAGIC - Obstacles spawn randomly with minimum safe gap between them
# MAGIC - High score persists during the session and shown at top
# MAGIC - Score increments every frame and displays as integer
# MAGIC
# MAGIC Make the code complete, single file, and immediately runnable with no missing assets.

# COMMAND ----------

import requests, os

host  = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")

response = requests.get(
    f"{host}/api/2.0/serving-endpoints",
    headers={"Authorization": f"Bearer {token}"}
)
import json
data = response.json()
print("Available endpoints:")
for ep in data.get("endpoints", []):
    print(" -", ep["name"])