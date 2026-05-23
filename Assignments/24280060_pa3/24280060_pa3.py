# Databricks notebook source
# MAGIC %md
# MAGIC # PA3

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC %pip install langchain_community
# MAGIC %pip install langgraph
# MAGIC dbutils.library.restartPython()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 0: Infrastructure Provisioning

# COMMAND ----------

import os
import re
import time
from dotenv import load_dotenv
import numpy as np
import pandas as pd
from typing import Iterator
from dotenv import load_dotenv
from openai import AzureOpenAI
from langchain_openai import AzureChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_core.prompts import (
    ChatPromptTemplate, 
    SystemMessagePromptTemplate, 
    HumanMessagePromptTemplate
)
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql.functions import udf, explode, col, pandas_udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

load_dotenv()

# THE ABOVE IS WHAT YOU NEED FOR THIS ASSIGNMENT

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: RAG Implementation from Scratch

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 0: Infrastructure Provisioning

# COMMAND ----------

# MAGIC %sql
# MAGIC Create catalog if not exists 24280060_pa3;
# MAGIC use catalog 24280060_pa3;

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema default;
# MAGIC create volume if not exists rag_documents;
# MAGIC create volume if not exists text_documents;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists parsed_data;
# MAGIC create schema if not exists knowledge_base_data;
# MAGIC create schema if not exists processed_data;
# MAGIC create schema if not exists vector_index;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Constructing the Knowledge Base

# COMMAND ----------

# DBTITLE 1,parsed documents table
# MAGIC %sql
# MAGIC create table if not exists parsed_data.parsed_documents (
# MAGIC     document_path string,
# MAGIC     parsed_document string
# MAGIC );

# COMMAND ----------

from pyspark.sql.functions import ai_parse_document

# COMMAND ----------

# DBTITLE 1,ai_parse_document
my_pdf = spark.read.format("binaryFile").load("/Volumes/24280060_pa3/default/rag_documents/")
my_pdf.createOrReplaceTempView("pdf_files")

ai_parsing = spark.sql("""
    SELECT
        path AS document_path,
        aggregate(
            transform(
                from_json(
                    cast(ai_parse_document(path) as string),
                    'document struct<elements: array<struct<content: string>>>'
                ).document.elements,
                el -> el.content
            ),
            cast('' as string),
            (acc, x) -> concat(acc, '\n', x)
        ) AS parsed_document
    FROM pdf_files
""")

ai_parsing.write.mode("append").saveAsTable("`24280060_pa3`.`parsed_data`.`parsed_documents`")


# COMMAND ----------

display(spark.table("parsed_data.parsed_documents"))

# COMMAND ----------

# MAGIC %sql
# MAGIC  CREATE TABLE IF NOT EXISTS 24280060_pa3.knowledge_base_data.knowledge_base(
# MAGIC         Id          BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC         Title       STRING,
# MAGIC         Authors     STRING,
# MAGIC         Content     STRING,
# MAGIC         DocumentURI STRING
# MAGIC     )
# MAGIC     USING DELTA
# MAGIC     TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')

# COMMAND ----------

spark.sql("""
    INSERT INTO `24280060_pa3`.`knowledge_base_data`.`knowledge_base`
        (Title, Authors, Content, DocumentURI)
    SELECT
        extracted.Title,
        extracted.Authors,
        extracted.Content,
        document_path AS DocumentURI
    FROM (
        SELECT
            document_path,
            ai_extract(
                parsed_document,
                array('Title', 'Authors', 'Content')
            ) AS extracted
        FROM `24280060_pa3`.`parsed_data`.`parsed_documents`
        WHERE parsed_document IS NOT NULL
    )
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 24280060_pa3.knowledge_base_data.knowledge_base

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Implement Chunking Strategies

# COMMAND ----------

class MyTextSplitter():
    def __init__(self, chunk_size=512, chunk_overlap=0):
        ### TO-DO: Implement the constructor for MyTextSplitter
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        pass
    
    def split_text(self, text):
        chunks = []
        ### TO-DO: Implement the split_text method
        start  = 0
        while start < len(text):
            end = start + self.chunk_size
            chunks.append(text[start:end])
            start += self.chunk_size - self.chunk_overlap
        return chunks

class MySemanticSplitter:
    def __init__(self, client, model_name, distance_threshold=0.3, buffer_size=1):
        ### TO-DO: Implement the constructor for MySemanticSplitter
        self.client = client
        self.model_name = model_name
        self.distance_threshold = distance_threshold
        self.buffer_size = buffer_size
        pass
    
    def _combine_sentences(self, sentences, buffer_size):
        combined = []
        ### TO-DO: Implement the _combine_sentences method
        for i in range(len(sentences)):
            start = max(0, i - buffer_size)
            end = min(len(sentences), i + buffer_size + 1)
            combined.append(" ".join(sentences[start:end]))
        return combined

    @staticmethod
    def cosine_similarity(embeddings1, embeddings2):
        ### TO-DO: Implement the cosine_similarity method
        dot_product = np.dot(embeddings1, embeddings2)
        norm1 = np.linalg.norm(embeddings1)
        norm2 = np.linalg.norm(embeddings2)
        cosine_similarities = dot_product / (norm1 * norm2+ 1e-10)

        return cosine_similarities

    def _get_embedding(self, text):
        ### TO-DO: Implement the _get_embedding method
        response = self.client.embeddings.create(
        input = text,
        model = self.model_name
        )
        embedding = np.array(response.data[0].embedding)
        return embedding

    def split_text(self, text):
        chunks = []
        ### TO-DO: Implement the split_text method

# split the sentences
        sentences = re.split(r'(?<=[.!?])\s+', text.strip())
        sentences = [s.strip() for s in sentences if s.strip()]

        if len(sentences) == 0:
            return chunks
        if len(sentences) == 1:
            return sentences

# combine each sentence
        combined = self._combine_sentences(sentences, self.buffer_size)

#generate the embeddings
        embeddings = [self._get_embedding(c) for c in combined]

# cpompute cosine similarity
        distances = []
        for i in range(len(embeddings) - 1):
            sim = self.cosine_similarity(embeddings[i], embeddings[i + 1])
            distances.append(1 - sim)

# grouping based on distance threshold
        current_chunk = [sentences[0]]
        for i, distance in enumerate(distances):
            if distance > self.distance_threshold:
                chunks.append(" ".join(current_chunk))
                current_chunk = [sentences[i + 1]]
            else:
                current_chunk.append(sentences[i + 1])

        if current_chunk:
            chunks.append(" ".join(current_chunk))

        return chunks
    

# COMMAND ----------

from dotenv import load_dotenv
import os
## my env file was not being read to i used the params directly 

os.environ["AZURE_OPENAI_API_KEY"] = "FPHHBeFRnB9McqCKfJ8NOC0FuHaB46OM4lWvcy8tB7DqNC4MmFnAJQQJ99CCACfhMk5XJ3w3AAABACOGLqpL"
os.environ["AZURE_OPENAI_ENDPOINT"] = "https://aoai-foundry-swc.openai.azure.com/"
os.environ["AZURE_OPENAI_API_VERSION"] = "2025-01-01-preview"
os.environ["AZURE_OPENAI_EMBEDDING_DEPLOYMENT"] = "text-embedding-3-small"
os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT"] = "lums-gpt-4.1-mini"

def get_client():
    return AzureOpenAI(
        api_key = os.environ["AZURE_OPENAI_API_KEY"],
        azure_endpoint = os.environ["AZURE_OPENAI_ENDPOINT"],
        api_version = os.environ["AZURE_OPENAI_API_VERSION"]
    )

# COMMAND ----------

### SANITY CHECK ###

# Sample Research Paper
sample_document = (
    "Abstract: The exponential growth in data has created a need for platforms capable of storing both structured & unstructured data, effectively processing the data, analyzing, and creating machine learning models. Traditional data lakes and warehouses often lack the flexibility and performance to provide these capabilities. So, the new Lakehouse paradigm is introduced. Databricks is an implementation of the Lakehouse paradigm that is cloud-native and built upon Apache Spark. However, there is a lack of substantial academic work describing the ecosystem. This paper presents a comprehensive description of the Databricks ecosystem, showing it both as an architecture and as a platform already in use. We will go through the main components of Databricks architecture, including Delta Lake, Unity Catalog, cluster management, Azure integrations, and discuss their roles in creating secure, scalable, and cost-effective data engineering workflows. We also present an experiment that is designed to bridge the gap between theory and practice by demonstrating the ingestion of data, feature engineering, and basic fraud detection. We utilized a synthetic dataset of financial transactions. The experimental procedure and metrics such as query latency, processing throughput, speed, and performance results are described in detail offering a reproducible benchmark for evaluating similar workloads in Databricks. This work is designed to function as a technical resource for individuals in industry, data engineers, and researchers who are interested in working with the Databricks environment for large-scale analytics. Keywords: Databricks, Lakehouse, Delta Lake, Data Engineering, PySpark, Unity Catalog, MLflow, Azure Synapse, Data Factory, Power BI, Real-time Analytics, Cloud Data Platforms, Fraud Detection, Performance Benchmarking, Industry Use Case. 1. Introduction These days organizations have high volume of data in the form of structured, semistructured and unstructured data. They need platforms that can integrate storage, analytics and machine learning at scale in order to gain value from their data. Conventional data warehouses store structured data for analytical needs (such as reporting through Power BI), but lack flexibility. Data lakes, on the other hand, can store semistructured and unstructured data using batch data processing, but do not provide ACID guarantees. The new Lakehouse model provides the best of both worlds to store and..."
)

# --- Test 1: The Normal Text Splitter (Fixed Size) ---
print("====== FIXED-SIZE CHUNKER ======")
normal_splitter = MyTextSplitter(chunk_size=512, chunk_overlap=50)
normal_chunks = normal_splitter.split_text(sample_document)

for i, chunk in enumerate(normal_chunks[:3]):
    print(f"\n--- Chunk {i+1} (Length: {len(chunk)}) ---")
    print(chunk)


# --- Test 2: The Semantic Splitter (Embedding-Based) ---
print("\n\n====== SEMANTIC CHUNKER ======")
semantic_splitter = MySemanticSplitter(
    client=get_client(), 
    model_name=os.environ["AZURE_OPENAI_EMBEDDING_DEPLOYMENT"], 
    distance_threshold=0.15, 
    buffer_size=1
)
semantic_chunks = semantic_splitter.split_text(sample_document)

for i, chunk in enumerate(semantic_chunks):
    print(f"\n--- Chunk {i+1} (Length: {len(chunk)}) ---")
    print(chunk)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# COMMAND ----------

fixed_splitter = MyTextSplitter(chunk_size=512, chunk_overlap=50)

# 1. Load the knowledge base table
knowledge_base_rows = spark.table("`24280060_pa3`.`knowledge_base_data`.`knowledge_base`").filter("Content IS NOT NULL").collect()
# 2. Split the content into chunks
fixed_rows = []

for row in knowledge_base_rows:
    doc_id = row["Id"]
    content = row["Content"]

    # Fixed chunks
    for idx, chunk in enumerate(fixed_splitter.split_text(content)):
        fixed_rows.append(Row(document_id=doc_id, chunk_index=idx, chunk_text=chunk))

print(f"Fixed chunks : {len(fixed_rows)}")


# 3. Write the chunks to a new table

chunk_schema = StructType([
    StructField("document_id", LongType(),    False),
    StructField("chunk_index", IntegerType(), False),
    StructField("chunk_text",  StringType(),  True),
])

# Fixed chunks
spark.sql("DROP TABLE IF EXISTS `24280060_pa3`.`processed_data`.`fixed_chunks`")
spark.createDataFrame(fixed_rows, chunk_schema) \
     .write.mode("overwrite") \
     .saveAsTable("`24280060_pa3`.`processed_data`.`fixed_chunks`")



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from 24280060_pa3.processed_data.fixed_chunks

# COMMAND ----------

client = AzureOpenAI(
    api_version=os.environ.get("AZURE_OPENAI_API_VERSION"),
    azure_endpoint=os.environ.get("AZURE_OPENAI_ENDPOINT"),
)

semantic_splitter = MySemanticSplitter(
    client=client,
    model_name=os.environ.get("AZURE_OPENAI_EMBEDDING_DEPLOYMENT"),
    distance_threshold=0.15
)

# 1. Load the knowledge base table

knowledge_base_rows = spark.table("`24280060_pa3`.`knowledge_base_data`.`knowledge_base`").filter("Content IS NOT NULL").collect()
# 2. Split the content into chunks
semantic_rows = []

for row in knowledge_base_rows:
    doc_id = row["Id"]
    content = row["Content"]

    for idx, chunk in enumerate(semantic_splitter.split_text(content)):
        semantic_rows.append(Row(document_id=doc_id, chunk_index=idx, chunk_text=chunk))

print(f"Semantic chunks: {len(semantic_rows)}")
# 3. Write the chunks to a new table

chunk_schema = StructType([
    StructField("document_id", LongType(),    False),
    StructField("chunk_index", IntegerType(), False),
    StructField("chunk_text",  StringType(),  True),
])


spark.sql("DROP TABLE IF EXISTS `24280060_pa3`.`processed_data`.`semantic_chunks`")
spark.createDataFrame(semantic_rows, chunk_schema) \
     .write.mode("overwrite") \
     .saveAsTable("`24280060_pa3`.`processed_data`.`semantic_chunks`")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 24280060_pa3.processed_data.semantic_chunks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Generating Vector Embeddings

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType

# COMMAND ----------

class GenerateEmbeddings:
    def __init__(self, client, model_name, chunk_table_name):
        ### TO-DO: Implement the __init__ method
        self.client = client
        self.model_name = model_name
        self.chunk_table_name = chunk_table_name
        self.embeddings = None
        pass

    def _get_embedding(self, text):
        response = self.client.embeddings.create(
            input = text,
            model = self.model_name
        )
        return response.data[0].embedding
    
    def embed(self):
        records = []
        ### TO-DO: Implement the embed method
        rows = spark.table(self.chunk_table_name).collect()

        for row in rows:
            chunk_text = row["chunk_text"]
            embedding  = self._get_embedding(chunk_text)
            records.append((chunk_text, embedding))

        schema = StructType([
            StructField("ChunkText", StringType(), True),
            StructField("Embedding", ArrayType(FloatType()), True),
        ])

        self.embeddings = spark.createDataFrame(records, schema=schema)

    def save_to_table(self, table_name):
        ### DONE: Saves the embeddings to a Delta table
        self.embeddings.write.format("delta").mode("overwrite").saveAsTable(table_name)



# COMMAND ----------

### TO-DO: Store the Fixed-Size Embeddings in a Delta Table
fixed_embeddings = GenerateEmbeddings(
    client = client,
    model_name = os.environ.get("AZURE_OPENAI_EMBEDDING_DEPLOYMENT"),
    chunk_table_name = "`24280060_pa3`.`processed_data`.`fixed_chunks`"
)
fixed_embeddings.embed()
fixed_embeddings.save_to_table("`24280060_pa3`.`processed_data`.`fixed_embeddings`")

# COMMAND ----------

### TO-DO: Store the Semantic Embeddings in a Delta Table
semantic_embeddings = GenerateEmbeddings(
    client = client,
    model_name = os.environ.get("AZURE_OPENAI_EMBEDDING_DEPLOYMENT"),
    chunk_table_name = "`24280060_pa3`.`processed_data`.`semantic_chunks`")
semantic_embeddings.embed()
semantic_embeddings.save_to_table("`24280060_pa3`.`processed_data`.`semantic_embeddings`")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 24280060_pa3.processed_data.fixed_embeddings;
# MAGIC select * from 24280060_pa3.processed_data.semantic_embeddings;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Simulating Vector Search

# COMMAND ----------

class VectorSearch:
    def __init__(self, client, model_name, embeddings_table):
        ### TO-DO: Implement the __init__ method
        self.client = client
        self.model_name = model_name
        self.embeddings_table = embeddings_table
        pass

    def _get_embedding(self, text):
        response = self.client.embeddings.create(
            input = text,
            model = self.model_name
        )
        return np.array(response.data[0].embedding)

    @staticmethod
    def cosine_similarity(vec1, vec2):
        return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2) + 1e-10)

    def search_vectors(self, query, top_k=3):
        ### TO-DO: Implement the search_vectors method
        ###     1. Embed the query
        ###     2. Calculate the cosine similarity between the query and each embedding
        ###     3. Sort the embeddings by similarity and return the top_k
        my_fetched_embedding = self._get_embedding(query)
        rows = spark.table(self.embeddings_table).collect()
        results = []
        for row in rows:
            chunk_embedding = np.array(row["Embedding"])
            similarity = self.cosine_similarity(my_fetched_embedding, chunk_embedding)
            results.append(Row(
                Similarity = float(similarity),
                Chunk_text = row["ChunkText"]
            ))

        results = sorted(results, key=lambda x: x.Similarity, reverse=True)[:top_k]

        return spark.createDataFrame(results)


# COMMAND ----------

vs_fixed   = VectorSearch(
    client = client,
    model_name = os.environ.get("AZURE_OPENAI_EMBEDDING_DEPLOYMENT"),
    embeddings_table = "`24280060_pa3`.`processed_data`.`fixed_embeddings`"
)
results_1 = vs_fixed.search_vectors("What is SearchAgent-X?")
display(results_1)


# COMMAND ----------

vs_semantic = VectorSearch(
    client = client,
    model_name = os.environ.get("AZURE_OPENAI_EMBEDDING_DEPLOYMENT"),
    embeddings_table = "`24280060_pa3`.`processed_data`.`semantic_embeddings`"
)
results_2   = vs_semantic.search_vectors("What is SearchAgent-X?")
display(results_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: RAG Architecture

# COMMAND ----------


llm = AzureChatOpenAI(
    azure_endpoint=os.environ.get("AZURE_OPENAI_ENDPOINT"),
    azure_deployment=os.environ.get("AZURE_OPENAI_CHAT_DEPLOYMENT"), 
    api_version=os.environ.get("AZURE_OPENAI_API_VERSION"), 
    temperature=0.0
)

### TO-DO
system_message = """You are a helpful assistant that answers questions strictly based on the provided context.If the context does not contain enough information to answer the question, respond with exactly: "Sorry, I do not know. Do not use any prior knowledge or information outside the context."
Context:{context}"""


### TO-DO
human_message = "Question: {question}"


### TO-DO
chat_prompt = ChatPromptTemplate.from_messages([("system", system_message), ("human",  human_message)])

class RAGPipeline:
    def __init__(self, llm, chat_prompt, vector_search):
        ### TO-DO: Implement the __init__ method
        self.llm = llm
        self.chat_prompt = chat_prompt
        self.vector_search = vector_search
        pass
    
    def get_context(self, question):
        ### TO-DO: Implement the get_context method. Join each chunks with 2 newline seperators
        results= self.vector_search.search_vectors(question)
        chunks= [row["Chunk_text"] for row in results.collect()]
        return "\n\n".join(chunks)

    def invoke(self, question):
        content = None
        ### TO-DO: Implement the invoke method
        context = self.get_context(question)
        formatted = self.chat_prompt.format_messages(
            context  = context,
            question = question
        )
        response = self.llm.invoke(formatted)
        content  = response.content
        return content

    def invoke_w_lcel(self, question):
        ### TO-DO: Implement the same functionality but use the LCEL
        context  = self.get_context(question)
        chain = (RunnablePassthrough() | (lambda x: {"context": x["context"], "question": x["question"]}) | self.chat_prompt | self.llm | StrOutputParser())

        return chain.invoke({
            "context"  : context,
            "question" : question
        })

# COMMAND ----------

rag_fixed = RAGPipeline(llm, chat_prompt, vs_fixed)
query = "What is SearchAgent-X?"
answer = rag_fixed.invoke(query)
print(f"Question: {query}")
print(f"Answer: {answer}")

print()

answer = rag_fixed.invoke_w_lcel(query)
print(f"Question: {query}")
print(f"Answer: {answer}")

# COMMAND ----------


rag_semantic = RAGPipeline(llm, chat_prompt, vs_semantic)
query = "What is SearchAgent-X?"
answer = rag_semantic.invoke(query)
print(f"Question: {query}")
print(f"Answer: {answer}")

print()

answer = rag_semantic.invoke_w_lcel(query)
print(f"Question: {query}")
print(f"Answer: {answer}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze both semantic and fixed chunking methods and the results they give. Do the answers differ? In what way are they alike and different? (MARKDOWN)
# MAGIC
# MAGIC Both pipelines returned **identical answers** for "What is SearchAgent-X?"
# MAGIC
# MAGIC  - there i only 1 document in the knowledge base thus both the strategies pull the data from same source
# MAGIC  - "SearchAgent-X" appears throughout the paper so both chunk types score similarly
# MAGIC
# MAGIC
# MAGIC  Differences emerge with **larger knowledge bases and specific technical queries**.
# MAGIC  Semantic chunking is superior in production RAG whereas the fixed chunking is a fast.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Databricks Managed Vector Search and Agentic RAG Flow

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Data Preparation

# COMMAND ----------

import os
from typing import TypedDict
from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, START, END
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableLambda, RunnablePassthrough
from databricks.vector_search.client import VectorSearchClient
from dotenv import load_dotenv

load_dotenv()

# COMMAND ----------

### TO-DO: Prepare the fixed chunks table

fixed_chunks_prepared = (
    spark.table("`24280060_pa3`.`processed_data`.`fixed_chunks`")
    .withColumn("Chunk_Id", expr("uuid()"))
)

fixed_chunks_prepared.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("`24280060_pa3`.`processed_data`.`fixed_chunks_prepared`")

spark.sql("""
    ALTER TABLE `24280060_pa3`.`processed_data`.`fixed_chunks_prepared`
    SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# COMMAND ----------

spark.table("`24280060_pa3`.`processed_data`.`fixed_chunks_prepared`").show(3, truncate=80)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Vector Search Management

# COMMAND ----------

# FUNCTIONS I MADE IN CASE YOU GUYS NEED TO CLEANUP YOUR WORKSPACE

def vector_store_endpoint_cleanup(vsc):
    endpoints = vsc.list_endpoints().get('endpoints', [])

    if not endpoints:
        print("No endpoints found. Workspace is clear.")
    else:
        for e in endpoints:
            name = e['name']
            print(f"Deleting endpoint: {name}...")
            vsc.delete_endpoint(name)
        
        print("Waiting for workspace quota to refresh...")
        while len(vsc.list_endpoints().get('endpoints', [])) > 0:
            print("Endpoint still terminating... waiting 5s", end="\r")
            time.sleep(5)
        print("\nWorkspace cleared. Quota reset to 0/1.")

def vector_store_index_cleanup(vsc, endpoint_name, index_full_name):
    """Delete the vector store endpoint if it exists"""
    try:
        print(f"Attempting to delete index: {index_full_name}...")
        vsc.delete_index(endpoint_name=endpoint_name, index_name=index_full_name)
        print("Index deletion command sent.")
    except Exception as e:
        print(f"Index not found or already deleted: {e}")

    try:
        print(f"Deleting endpoint: {endpoint_name}...")
        vsc.delete_endpoint(name=endpoint_name)
    except Exception as e:
        print(f"Endpoint not found or already deleted: {e}")

    print("Waiting for Unity Catalog to clear metadata...waiting 5s")
    time.sleep(5)
    print("System cleared. You can now run the creation script.")


# COMMAND ----------

vsc = VectorSearchClient()
endpoint_name = "24280060-pa3-endpoint"
index_name = "24280060_pa3.processed_data.fixed_chunks_prepared_index"

# COMMAND ----------

# vector_store_endpoint_cleanup(vsc)
# vector_store_index_cleanup(vsc, endpoint_name, index_name)

# COMMAND ----------


endpoint_name = "24280060_pa3_vector_endpoint"
index_name    = "24280060_pa3.vector_index.fixed_vector_index"

# COMMAND ----------

print(f"Creating Endpoint: {endpoint_name}...")
### TO-DO: Create the vector store endpoint
vsc.create_endpoint(
    name = endpoint_name,
    endpoint_type = "STANDARD"
)

print(f"\nCreating index: {index_name}...")
### TO-DO: Create the vector store index

print("Index creation initiated.")
vsc.create_delta_sync_index(
    endpoint_name = endpoint_name,
    index_name= index_name,
    source_table_name = "24280060_pa3.processed_data.fixed_chunks_prepared",
    pipeline_type = "TRIGGERED",
    primary_key = "Chunk_Id",
    embedding_source_column= "chunk_text",
    embedding_model_endpoint_name = "databricks-gte-large-en"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Adaptive RAG Workflow

# COMMAND ----------

llm = AzureChatOpenAI(
    azure_endpoint=os.environ.get("AZURE_OPENAI_ENDPOINT"),
    azure_deployment=os.environ.get("AZURE_OPENAI_CHAT_DEPLOYMENT"), 
    api_version=os.environ.get("AZURE_OPENAI_API_VERSION"), 
    temperature=0.0
)

class GraphState(TypedDict):
    question: str
    initial_answer: str
    grade: str
    final_answer: str
    context: str

# COMMAND ----------

def retrieve_formatted_context(question: str, topk: int = 3):
    ### TO-DO: Retrieve the top-k most relevant chunks from the vector store
    results = (
        VectorSearchClient().get_index(
            endpoint_name = endpoint_name,
            index_name= index_name
        ).similarity_search(
            query_text = question,
            columns= ["chunk_text"],
            num_results= topk
        )
    )
    chunks = [row[0] for row in results.get("result", {}).get("data_array", [])]
    return "\n\n".join(chunks)

### TO-DO
context_retriever_node =RunnableLambda(lambda x: retrieve_formatted_context(x["question"]))

### TO-DO
system_message = """You are a helpful assistant that answers questions strictly based on the provided context.If the context is insufficient, respond with exactly: "I do not know". Do not use any prior knowledge outside the context.

Context:
{context}"""

### TO-DO
human_message = "Question: {question}"

### TO-DO
chat_prompt = ChatPromptTemplate.from_messages([("system", system_message), ("human",  human_message)])

### TO-DO
rag_chain = ({
    "context" : RunnableLambda(lambda x: retrieve_formatted_context(x["question"])),
    "question": RunnablePassthrough() | RunnableLambda(lambda x: x["question"])} |chat_prompt | llm | StrOutputParser()
)

def base_rag_node(state: GraphState) -> dict:
    ### TO-DO
    question= state["question"]
    context = retrieve_formatted_context(question)
    initial_answer = rag_chain.invoke({"question": question})
    return {
        "context": context,
        "initial_answer": initial_answer
    }



# COMMAND ----------

### TO-DO
class AnswerGrader(BaseModel):
    binary_score: str = Field(description="Relevance score: 'yes' if the answer is relevant and grounded, 'no' otherwise")

### TO-DO
grader_system_message = """You are a grader evaluating whether an answer is relevant and grounded 
in the provided context. Respond with a binary score:
- 'yes' if the answer is relevant and supported by the context
- 'no' if the answer is irrelevant, incomplete, or says 'I do not know'"""

### TO-DO
grader_human_message =  """Context: {context}
Question: {question}
Answer: {initial_answer}

Is the answer relevant and grounded in the context?"""

### TO-DO
grader_prompt = ChatPromptTemplate.from_messages([
    ("system", grader_system_message),
    ("human",grader_human_message)
])

### TO-DO
grader_chain = grader_prompt | llm.with_structured_output(AnswerGrader)


def grader_node(state: GraphState) -> dict:
    ### TO-DO
    result = grader_chain.invoke({
        "context"        : state["context"],
        "question"       : state["question"],
        "initial_answer" : state["initial_answer"]
    })
    return {"grade": result.binary_score}



# COMMAND ----------

### TO-DO
class CategoryRouter(BaseModel):
    category: str = Field(description="Category of the question: 'databricks' or 'azure'")

### TO-DO
router_prompt =ChatPromptTemplate.from_template(
    """You are a router that classifies a question into one of two categories:
- 'databricks': if the question is about Databricks, Delta Lake, Spark, Unity Catalog, or MLflow
- 'azure': if the question is about Azure services, Azure OpenAI, Azure Synapse, or cloud infrastructure

Question: {question}

Respond with only the category name."""
)

### TO-DO
router_chain = router_prompt | llm.with_structured_output(CategoryRouter)


def text_file_fallback_node(state: GraphState) -> dict:
    ### TO-DO
    question = state["question"]
    category = router_chain.invoke({"question": question}).category.lower()

    
    file_map = {
       "databricks": "/Volumes/24280060_pa3/default/text_documents/databricks_info.txt",
        "azure" : "/Volumes/24280060_pa3/default/text_documents/azure_info.txt",
    }
    
    file_path = file_map.get(category, None)
    if file_path is None:
        return {"final_answer": f"Category not found: {category}"}
    
    try:
        with open(file_path, "r") as f:
            full_text = f.read()
            
        response = llm.invoke(
            ### TO-DO
            f"Using the following document, answer this question:\n\n"
            f"Question: {question}\n\n"
            f"Document:\n{full_text}"
        )
        
        return {"final_answer": response.content}
        
    except Exception as e:
        return {"final_answer": f"Fallback failed. Could not read file at {file_path}. Error: {e}"}


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Creating the Agentic RAG Graph

# COMMAND ----------


### TO-DO: Using these functions, create a state graph that will answer, grade, reanswer a questions
def route_based_on_grade(state: GraphState) -> str:
    return "end" if state["grade"] == "yes" else "fallback"

workflow = StateGraph(GraphState)

workflow.add_node("local_rag_node", base_rag_node)
workflow.add_node("grader_node", grader_node)
workflow.add_node("file_fallback_node", text_file_fallback_node)

workflow.add_edge(START, "local_rag_node")
workflow.add_edge("local_rag_node", "grader_node")

workflow.add_conditional_edges(
    "grader_node",
    route_based_on_grade,
    {
        "end" : END,
        "fallback": "file_fallback_node"
    }
)

workflow.add_edge("file_fallback_node", END)

compiled_adaptive_rag = workflow.compile()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Executing tbe Agentic RAG FLow

# COMMAND ----------

def agentic_rag_flow(question: str) -> str:
    inputs     = {"question": question}
    full_state = {"question": question}

    for step in compiled_adaptive_rag.stream(inputs):
        for node_name, state_update in step.items():
            print(f"-----NODE: {node_name.upper()}-----")
            full_state.update(state_update)

            if "context" in state_update:
                print(f"Context Preview : {str(state_update['context'])[:200]}...")
            if "initial_answer" in state_update:
                print(f"Initial Answer : {state_update['initial_answer']}")
            if "grade" in state_update:
                print(f"Grade : {state_update['grade']}")
            if "final_answer" in state_update:
                print(f"Final Answer : {state_update['final_answer']}")
            print()

    final_answer = (
        full_state.get("final_answer") or
        full_state.get("initial_answer") or
        "No answer could be determined."
    )

    print("=" * 50)
    print(f"FINAL ANSWER: {final_answer}")
    print("=" * 50)

    return final_answer

# COMMAND ----------


inputs = {"question": "What is Databricks?"}
final_state = None

print(f"--- STARTING ADAPTIVE RAG FLOW ---")
print(f"User Question: {inputs['question']}\n")

### TO-DO
agentic_rag_flow(inputs["question"])

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/24280060_pa3/default/text_documents/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## MCP

# COMMAND ----------

# MAGIC %pip install --upgrade mlflow[databricks] databricks-agents
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import time
import yaml
import mlflow
from dotenv import load_dotenv
from mlflow.pyfunc import ResponsesAgent
from typing import Any, Dict
from langchain_openai import AzureChatOpenAI
from langchain_classic.agents import create_tool_calling_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate
from databricks.vector_search.client import VectorSearchClient
from langchain_core.tools import StructuredTool         
load_dotenv()


# COMMAND ----------

config_content = """llm_endpoint: "lums-gpt-4.1-mini"
mcp_servers:
  - name: databricks_managed_mcp
    url: https://adb-1234567890.azuredatabricks.net/api/2.0/mcp/functions/system/ai
"""

config_path = "/Volumes/24280060_pa3/default/text_documents/agent_config.yml"

with open(config_path, "w") as f:
    f.write(config_content)

print("agent_config.yml written to:", config_path)

with open(config_path, "r") as f:
    print(f.read())

# COMMAND ----------

workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
os.environ["DATABRICKS_HOST"]  = f"https://{workspace_url}"
os.environ["DATABRICKS_TOKEN"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

os.environ["AZURE_OPENAI_API_KEY"]= "FPHHBeFRnB9McqCKfJ8NOC0FuHaB46OM4lWvcy8tB7DqNC4MmFnAJQQJ99CCACfhMk5XJ3w3AAABACOGLqpL"
os.environ["AZURE_OPENAI_ENDPOINT"] = "https://aoai-foundry-swc.openai.azure.com/"
os.environ["AZURE_OPENAI_API_VERSION"]= "2025-01-01-preview"
os.environ["AZURE_OPENAI_EMBEDDING_DEPLOYMENT"] = "text-embedding-3-small"
os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT"]= "lums-gpt-4.1-mini"



# COMMAND ----------

def vector_search_tool(query: str) -> str:
    ### TO-DO: Implement a function that calls the vector store endpoint and write a detailed docstring for the function.
    """Performs similarity search against the Databricks Vector Search index
    and returns the top matching document chunks as a formatted string."""
    try:
        results = (
            VectorSearchClient().get_index(
                endpoint_name = endpoint_name,
                index_name = index_name).similarity_search(
                query_text  = query,
                columns= ["chunk_text"],
                num_results = 3))
        
        chunks = [row[0] for row in results.get("result", {}).get("data_array", [])]
        if not chunks:
            return "No relevant documents found."
        return "\n\n".join(chunks)
    except Exception as e:
        return f"Database search failed: {e}"

def vector_store_endpoint_cleanup(vsc):
    ### TO-DO: Implement a function that deletes the vector store endpoint if they exist and write a detailed docstring for the function
    """Deletes all existing Vector Search endpoints in the workspace."""
    vsc = VectorSearchClient()
    endpoints = vsc.list_endpoints().get("endpoints", [])
    if not endpoints:
        return "No endpoints found. Workspace is clear."
    for e in endpoints:
        name = e["name"]
        vsc.delete_endpoint(name)
    while len(vsc.list_endpoints().get("endpoints", [])) > 0:
        time.sleep(5)
    return "All endpoints deleted."



def vector_store_index_cleanup(vsc, endpoint_name, index_full_name):
    ### TO-DO: Implement a function that deletes the vector store index if they exist and write a detailed docstring for the function
    """Deletes a specific Vector Search index and its associated endpoint."""
    vsc = VectorSearchClient()
    vsc.delete_index(endpoint_name=endpoint_name, index_name=index_full_name)
    vsc.delete_endpoint(name=endpoint_name)
    time.sleep(5)
    return f"Index '{index_full_name}' and endpoint '{endpoint_name}' removed."
 


# COMMAND ----------


class VectorSearchAgent(ResponsesAgent):
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        super().__init__()
        
        self.system_prompt = (
            "You are a precise data engineering assistant. "
            "To answer questions about the course or documentation, you MUST use "
            "the provided vector_search_tool to retrieve context. "
            "If the tool does not return relevant information, state that you do not know."
        )

    def predict(self, payload) -> Dict[str, Any]:
        """Executes the Agent Loop locally."""
        if isinstance(payload, dict):
            messages = payload.get("messages", payload.get("input", []))
        elif isinstance(payload, list):
            messages = payload
        else:
            messages = getattr(payload, "messages", getattr(payload, "input", []))
            
        user_question = messages[-1]['content'] if isinstance(messages[-1], dict) else messages[-1].content
        
        ### TO-DO
        llm = AzureChatOpenAI(
            api_key= os.environ["AZURE_OPENAI_API_KEY"],
            azure_endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT"),
            azure_deployment = self.config.get("llm_endpoint"),
            api_version = os.environ.get("AZURE_OPENAI_API_VERSION"),
            temperature= 0.0
        )
        
        prompt_template = ChatPromptTemplate.from_messages([
            ("system",self.system_prompt),
            ("human","{input}"),
            ("placeholder", "{agent_scratchpad}")])
        
        
        tools = [
            StructuredTool.from_function(vector_search_tool),
            StructuredTool.from_function(vector_store_endpoint_cleanup),
            StructuredTool.from_function(vector_store_index_cleanup)
        ]        
        agent_logic  = create_tool_calling_agent(llm, tools, prompt_template)
        agent_executor = AgentExecutor(
            agent   = agent_logic,
            tools   = tools,
            verbose = True
        )
        
        # RUN THE LOOP
        raw_response = agent_executor.invoke({"input": user_question})["output"]
        
        return {"choices": [{"message": {"role": "assistant", "content": raw_response}}]}

agent = VectorSearchAgent(config_path="/Volumes/24280060_pa3/default/text_documents/agent_config.yml")
mlflow.models.set_model(model=agent)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Executing and Testing

# COMMAND ----------

test_questions = [
    "What is the Databricks Vector Search endpoint cleanup function supposed to do?",
    "What tools do you have and what do they do?"
]

print("Starting Local Agent Executor Test...\n" + "="*50)

for question in test_questions:
    print(f"\nUser Query: {question}")
    
    # Required schema structure
    payload = {
        "input": [{"role": "user", "content": question}], 
        "messages": [{"role": "user", "content": question}]
    }
    
    # Invoke the agent
    response = agent.predict(payload)
    final_answer = response["choices"][0]["message"]["content"]
    
    print(f"Final Answer:\n{final_answer}")
    print("-" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC - The agent processes queries through three nodes. In the first node LOCAL_RAG_NODE, it retrieves chunks from the vector store and generates an initial answer. For "What is Databricks?", it retrieved SearchAgent-X related chunks which were irrelevant, so the LLM responded "I do not know". For "What is SearchAgent-X?", it retrieved highly relevant chunks and generated a detailed answer.
# MAGIC - In the second node GRADER_NODE, the agent evaluates the initial answer using structured output AnswerGrader(binary_score='yes/no'). For Databricks the grade was no since the answer was insufficient, triggering the fallback route. For SearchAgent-X the grade was yes, routing directly to END.
# MAGIC - In the third node FILE_FALLBACK_NODE, only triggered when grade is no, the router classifies the query category using CategoryRouter(category='databricks'), opens the corresponding text file from the volume, and passes the full document to the LLM which generates a comprehensive final answer.
# MAGIC
# MAGIC
# MAGIC The grader acts as a quality gate when the vector store lacks relevant context the pipeline automatically falls back to pre-stored documents, ensuring the user always receives a meaningful answer regardless of whether the query falls within the vector store's domain.
# MAGIC