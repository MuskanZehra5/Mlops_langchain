# Databricks notebook source
# MAGIC %pip install langchain
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI
from langchain.tools import tool
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import create_react_agent
from databricks.vector_search.client import VectorSearchClient

load_dotenv()


# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `24280060_pa3`;
# MAGIC use schema `default`;

# COMMAND ----------

os.environ["AZURE_OPENAI_API_KEY"]= "FPHHBeFRnB9McqCKfJ8NOC0FuHaB46OM4lWvcy8tB7DqNC4MmFnAJQQJ99CCACfhMk5XJ3w3AAABACOGLqpL"
os.environ["AZURE_OPENAI_ENDPOINT"]= "https://aoai-foundry-swc.openai.azure.com/"
os.environ["AZURE_OPENAI_API_VERSION"]= "2025-01-01-preview"
os.environ["AZURE_OPENAI_EMBEDDING_DEPLOYMENT"] = "text-embedding-3-small"
os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT"]  = "lums-gpt-4.1-mini"

workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
os.environ["DATABRICKS_HOST"]  = f"https://{workspace_url}"
os.environ["DATABRICKS_TOKEN"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


# COMMAND ----------

endpoint_name = "24280060_pa3_vector_endpoint"
index_name    = "24280060_pa3.vector_index.fixed_vector_index"

# COMMAND ----------

def retrieve_formatted_context(question: str, topk: int = 3):
    results = VectorSearchClient().get_index(
        endpoint_name=endpoint_name,
        index_name=index_name).similarity_search(
        query_text=question,
        columns=["chunk"],
        num_results=topk
    )
    docs = ...
    raw_docs = [doc[1] for doc in docs if len(doc) > 1]
    return "\n\n---\n\n".join(raw_docs)


@tool
def vector_search_tool(query: str) -> str:
    # TO-DO: Write a docstring for this function
    # TO-DO: Implement the function
    """Searches the Databricks Vector Store index for relevant document chunks
    based on semantic similarity to the provided query. Use this tool first
    when answering questions about SearchAgent-X or the research paper content.
    Parameters:
        query (str): The search term or question to look up.
    Returns:
        str: Relevant document chunks or a not-found message.
    """
    try:
        return retrieve_formatted_context(query)
    except Exception as e:
        return f"Vector search failed: {e}"

@tool
def read_fallback_document(category: str) -> str:
    # TO-DO: Write a docstring for this function
    # TO-DO: Complete the function
    """Reads a pre-stored text document for a given category when vector search
    does not return relevant results. Supports 'databricks' and 'azure' categories.

    Parameters:
        category (str): The topic — either 'databricks' or 'azure'.

    Returns:
        str: Full text of the documentation file or an error message.
    """
    file_map = {
        "databricks": "/Volumes/24280060_pa3/default/text_documents/databricks_info.txt",
        "azure"      : "/Volumes/24280060_pa3/default/text_documents/azure_info.txt"
    }
    
    category = category.strip().lower()
    file_path = file_map.get(category)
    
    if not file_path:
        return f"No documentation found for category: {category}"
        
    try:
        with open(file_path, "r") as f:
            return f.read()
    except Exception as e:
        return f"Error reading document: {e}"
    

def build_stateful_mcp_agent():
    llm = AzureChatOpenAI(
        api_key = os.environ["AZURE_OPENAI_API_KEY"],
        azure_endpoint=os.environ.get("AZURE_OPENAI_ENDPOINT"),
        azure_deployment=os.environ.get("AZURE_OPENAI_CHAT_DEPLOYMENT"), 
        api_version=os.environ.get("AZURE_OPENAI_API_VERSION"), 
        temperature=0.0
    )
    
    tools = [vector_search_tool, read_fallback_document]
    memory = MemorySaver()
    
    mcp_agent = create_react_agent(model        = llm,
        tools = tools,
        checkpointer = memory,
        prompt = (
            "You are a precise data engineering assistant with access to tools. "
            "Always use vector_search_tool first to find relevant information. "
            "If vector search returns no useful results, use read_fallback_document "
            "with category 'databricks' or 'azure' as appropriate. "
            "Maintain context from previous messages in the conversation."))
    return mcp_agent

agent = build_stateful_mcp_agent()

# COMMAND ----------

# Summary: Executes queries against the agent while maintaining a consistent session thread.
def execute_mcp_queries(mcp_agent, queries: list, thread_id: str):
    config = {"configurable": {"thread_id": thread_id}}
    
    for query in queries:
        print(f"\nUser: {query}")
        payload = {"messages": [{"role": "user", "content": query}]}
        response = mcp_agent.invoke(payload, config=config)
        final_message = response["messages"][-1].content
        
        print(f"Agent: {final_message}")

# COMMAND ----------

validation_queries = [
    "What tools do you have?",
    "What is searchAgent-X?",
    "What does this agent do specifically?",
    "Use your tool to search for information about Azure.",
    "Based on those search results, summarize its main features.",
    "What is Databricks?",
    "What core features differentiate both products?"
]

print("--- TESTING STATEFUL MCP AGENT ---")
execute_mcp_queries(agent, validation_queries, "mcp_test_session_01")

# COMMAND ----------

