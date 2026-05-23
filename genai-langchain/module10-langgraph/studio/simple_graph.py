import sys
from pathlib import Path
from pprint import pprint

# Add project root to path
module_path = str(Path("../").resolve())
if module_path not in sys.path:
    sys.path.insert(0, module_path)

print(f"Added to Python path: {module_path}")

from llm_utils import setup, create_azure_llm, create_azure_embedding

setup()

from typing import TypedDict 
from langgraph.graph import StateGraph, END 

class AgentState(TypedDict):
    name: str 

def ask_name(state: AgentState) -> AgentState: 
    """ Prompts the user to enter their name and updates the state with it. """ 
    print("What's your name?") 
    name = input(">> ") 
    state["name"] = name 
    return state

def greet(state: AgentState) -> AgentState: 
    print(f"Hello, {state['name']}! Welcome to LangGraph.") 
    return state

graph = StateGraph(AgentState) 
graph.add_node("ask_name", ask_name) 
graph.add_node("greet", greet) 
graph.set_entry_point("ask_name") 
graph.add_edge("ask_name", "greet") 
graph.add_edge("greet",END) 

compiled_graph = graph.compile() 
print(compiled_graph.get_graph().draw_ascii()) 

# COMMAND ----------
compiled_graph.invoke({})


