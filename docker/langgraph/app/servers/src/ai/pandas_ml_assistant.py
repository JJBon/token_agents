import os
import sqlite3
from typing import Annotated, Literal, Tuple, Union

import aiosqlite
from langchain_core.messages import SystemMessage
from langchain_experimental.tools import PythonAstREPLTool
from langchain_ollama import ChatOllama
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.errors import GraphRecursionError
from langgraph.graph import END, MessagesState, START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt import create_react_agent
from ollama._types import ResponseError
import pandas as pd
from pydantic import BaseModel, Field, ValidationError

from servers.src.ai.agent_tools import (
    build_decision_tree_classifier,
    compute_accuracy_metrics,
    export_decision_tree_to_text,
    model_inference,
)
from servers.src.logger import logger
from servers.src.variables import (
    DATAFRAME_ASSISTANT_SAMPLE_CSV_PATH,
    OLLAMA_MODEL,
    SQLITE_CHECKPOINTER_DB_PATH,
    TEMPERATURE,
)

df = pd.read_csv(DATAFRAME_ASSISTANT_SAMPLE_CSV_PATH)

llm = ChatOllama(model=OLLAMA_MODEL, temperature=TEMPERATURE)


ValueType = Annotated[
    Literal["default", "pandas_agent", "ml_agent"],
    Field(
        description="Allowed values: 'default' (no additional context is needed), 'pandas_agent' (data analysis of a pandas dataframe "
                    "is required for an accurate answer), or 'ml_agent' (machine learning operations like model training or inference are required)"
    )
]


class RouterModel(BaseModel):
    value: ValueType


structured_llm = llm.with_structured_output(RouterModel)


class State(MessagesState):
    summary: str
    context: str


async def build_graph(
        return_memory: bool = False,
) -> Union[
    CompiledStateGraph,
    Tuple[CompiledStateGraph, SqliteSaver],
]:
    """
    Construct and return a compiled LangChain StateGraph tailored for conversational reasoning using conditional routing 
    between ML Agent and Pandas DataFrame Agent.

    Parameters
    ----------
    return_memory : bool, optional
        If True, also return the underlying SqliteSaver used to persist conversation state.

    Returns
    -------
    graph : CompiledStateGraph
        A compiled LangChain state graph that routes user messages through a conversation node to either Pandas DataFrame Agent or ML agent.
    graph, memory : Tuple[CompiledStateGraph, SqliteSaver]
        If `return_memory` is True, returns both the compiled StateGraph and the SqliteSaver for checkpointing and
        state persistence.
    """
    summary_thr = 10
    n_msg_overlap = 2

    def router_system_message(summary_msg: str):
        sys_msg = SystemMessage(content=f"""
            You are an intelligent decision-making agent tasked with routing user queries based on their content.
            Decide if the user's prompt requires specific capabilities to answer.
            Choose *one* of the following options based on the query:

            1. 'default'
            Use this if the user's query:
            - Is conversational or general-purpose
            - Involves common programming, math, or factual questions
            - Does *not* ask about the CSV's content or ML operations

            Example queries:
            * "What is the capital of France?"
            * "How do I define a class in Python?"
            * "Tell me about the weather today."

            2. 'pandas_agent'
            Use this if the user prompt queries data/statistics, specific columns, filtering, plotting, or aggregations from the CSV.
            
            Here are the dataframe columns:
            {df.columns.tolist()}
            
            TRIGGER PHRASES (non-exhaustive)
            - Route to 'pandas_agent' if query includes only: 
            "filter", "groupby", "aggregate", "sum", "mean", "median", "std", "count", "describe", "correlation", 
            "value_counts", "plot", "histogram", "boxplot", "line chart", "bar chart" applied to dataframe columns.
            
            Example queries:
            * "Find the three most popular materials."
            * "Describe the dataset."
            * "Calculate correlation between these two columns."

            3. 'ml_agent'
            Use this if the user wants to:
            - Train a decision tree model
            - Make inference or predictions using a trained model
            - Perform machine learning operations such as mentioning `build_decision_tree_classifier()` or `model_inference()` functions
            - Calculate accuracy metrics like precision, recall, f1-score, etc.

            TRIGGER PHRASES (non-exhaustive)
            - Route to 'ml_agent' if query includes: 
            "train", "retrain", "fit", "tune", "hyperparameters", "model", "pipeline", "predict", "prediction", "inference", 
            "score", "evaluate", "evaluation", "metrics", "precision", "recall", "f1", "f1-score", "micro", "macro", "weighted",
            "accuracy", "AUC", "ROC", "confusion matrix", "cross validation", "feature importance".
            
            Example queries:
            * "Train a decision tree model to predict Material"
            * "Build a model using target variable X"
            * "Make predictions for these feature values"
            * "Use the trained model to predict"

            ------------------------------------------------------------------------------------------------------

            {summary_msg}

            Your response: Output only one of the following options based on the user query: 'default', 'pandas_agent', or 'ml_agent'.
            """)

        return sys_msg

    # Node
    def conversation_node(state: State):
        filtered_messages = [
            msg for msg in state["messages"] if msg.type in {"human", "ai"} and msg.content
        ]
        n_msg = len(filtered_messages)

        if n_msg > summary_thr and (n_msg - 1) % summary_thr == 0:
            messages = filtered_messages[-(summary_thr + 1):]

            conversation_history = []
            for msg in messages:
                if msg.type == "human":
                    conversation_history.append(f"Human: {msg.content}")
                elif msg.type == "ai":
                    conversation_history.append(f"AI: {msg.content}")

            human_ai_messages = "\n".join([item for item in conversation_history[:-1]])
            summary = state.get("summary")
            if summary:
                summary_message = f"""
                This is a summary of the conversation so far: '{summary}'

                Update and extend the summary by incorporating only the most important points and outcomes from the new
                messages below. Keep the overall summary brief and focused. Limit the total length to 3-4 sentences.
                <start>
                {human_ai_messages}
                <end>
                """
            else:
                summary_message = f"""
                Generate a brief summary (3-4 sentences) of the conversation below, capturing only the most important 
                points and outcomes. Exclude minor details and direct quotes.
                <start>
                {human_ai_messages}
                <end>
                """

            response = llm.invoke(summary_message)

            return {"summary": response.content}

    # Node
    def pandas_agent_node(state: State):
        filtered_messages = [
            msg for msg in state["messages"] if msg.type in {"human", "ai"} and msg.content
        ]
        n_msg = len(filtered_messages)  

        # Create Python REPL tool with access to dataframe
        python_tool = PythonAstREPLTool(locals={
            "df": df
        })

        n_msg = len(state["messages"])

        if n_msg < summary_thr:
            messages = filtered_messages
        else:
            idx = (n_msg % summary_thr) + n_msg_overlap
            messages = filtered_messages[-idx:]

        if summary := state.get("summary"):
            summary_prompt = f"""
            For the reference here is a summary of conversation earlier: '{summary}'
            """
        else:
            summary_prompt = ""


        prompt = f"""You are a helpful assistant that has access to the tools below to answer user questions.

        Available tools:
        - python_tool: A Python shell with access to a pandas DataFrame (df) with the following columns:
        {df.columns.tolist()}

        IMPORTANT: When calling tools, you MUST use the exact JSON format:
        {{"query": "your_python_code_here"}}

        Example tool call:
        {{"query": "df.head()"}}

        Given a user question, write the Python code to answer it and wrap it in the proper JSON tool call format.

        If tools have been used, summarize their outputs clearly in your final response to the user.
        DO NOT respond until you have reviewed the results of any tool invocations.
        Please make your answer as concise as possible.

        {summary_prompt}
        """

        pandas_agent = create_react_agent(
            model=llm, 
            tools=[python_tool],
            prompt=prompt,
        )

        try:
            result = pandas_agent.invoke({"messages": messages}, {"recursion_limit": 10})
            return {"messages": result["messages"]}
        except ResponseError as e:
            logger.error(f"Ollama ResponseError while invoking 'pandas_agent': {e}")
            user_friendly_msg = (
                "⚠️ Something went wrong on my side. "
                "I couldn’t complete your request, please try again."
            )
            return {"messages": {"role": "assistant", "content": user_friendly_msg}}
        except GraphRecursionError as e:
            logger.error(f"GraphRecursionError occured while invoking 'pandas_agent': {e}")
            user_friendly_msg = (
                "Sorry, I ran out of reasoning steps (recursion limit reached). " 
                "Please rephrase or simplify your request."
            )
            return {"messages": {"role": "assistant", "content": user_friendly_msg}}

    # Node
    def ml_agent_node(state: State):
        filtered_messages = [
            msg for msg in state["messages"] if msg.type in {"human", "ai"} and msg.content
        ]
        n_msg = len(filtered_messages)  

        # Create Python REPL tool with access to ML functions
        python_tool = PythonAstREPLTool(locals={
            "build_decision_tree_classifier": build_decision_tree_classifier,
            "model_inference": model_inference,
            "compute_accuracy_metrics": compute_accuracy_metrics,
            "export_decision_tree_to_text": export_decision_tree_to_text
        })

        if n_msg < summary_thr:
            messages = filtered_messages
        else:
            idx = (n_msg % summary_thr) + n_msg_overlap
            messages = filtered_messages[-idx:]

        if summary := state.get("summary"):
            summary_prompt = f"""
            For the reference here is a summary of conversation earlier: '{summary}'
            """
        else:
            summary_prompt = ""


        prompt = f"""You are a helpful ML assistant that has access to the tools below to answer user questions.

        Available tools:
        - python_tool: A Python shell with access to a function 'build_decision_tree_classifier(target_variable, average)' for building decision tree models, 
        'model_inference(feature_values: Dict[str, list])' for using the trained decision tree model for making predictions, 'compute_accuracy_metrics(target_variable, average)' 
        for evaluating a trained decision tree model at user request, and 'export_decision_tree_to_text()' for exporting a trained decision tree model into a 
        human-readable text representation containing the decision rules of the tree, formatted as nested if/else statements.

        - To build a decision tree model call "build_decision_tree_classifier(target_variable)". In case target variable has two classes (0/1)
          make sure to call "build_decision_tree_classifier(target_variable, average='binary')".
        - To make inference call "model_inference(feature_values)"
        - Use "compute_accuracy_metrics(target_variable, average)" if user asks for accuracy scores for average 'micro', 'weighted', etc.
        - Use "export_decision_tree_to_text()" if user wants to visualize the trained decision tree.

        If tools have been used, summarize their outputs clearly in your final response to the user.
        Do not respond until you have reviewed the results of any tool invocations.

        {summary_prompt}
        """
        
        ml_agent = create_react_agent(
            model=llm, 
            tools=[python_tool],
            prompt=prompt,
        )

        try:
            result = ml_agent.invoke({"messages": messages}, {"recursion_limit": 10})
            return {"messages": result["messages"]}
        except ResponseError as e:
            logger.error(f"Ollama ResponseError while invoking 'ml_agent': {e}")
            user_friendly_msg = (
                "⚠️ Something went wrong on my side. "
                "I couldn’t complete your request, please try again."
            )
            return {"messages": {"role": "assistant", "content": user_friendly_msg}}     
        except GraphRecursionError as e:
            logger.error(f"GraphRecursionError occured while invoking 'ml_agent': {e}")
            user_friendly_msg = (
                "Sorry, I ran out of reasoning steps (recursion limit reached). " 
                "Please rephrase or simplify your request."
            )
            return {"messages": {"role": "assistant", "content": user_friendly_msg}}   


    def router(state: State) -> Literal['pandas_agent', 'ml_agent']:
        filtered_messages = [
            msg for msg in state["messages"] if msg.type in {"human", "ai"} and msg.content
        ]
        n_msg = len(filtered_messages)  

        if n_msg < summary_thr:
            messages = filtered_messages
        else:
            idx = (n_msg % summary_thr) + n_msg_overlap
            messages = filtered_messages[-idx:]

        if summary := state.get("summary"):
            summary_prompt = f"""
            For the reference here is a summary of conversation earlier: '{summary}'
            """
        else:
            summary_prompt = ""

        try:
            if OLLAMA_MODEL == "gpt-oss:20b":  # NOTE: we do not use here structured_llm as it breaks for this model!
                llm_output = llm.invoke([router_system_message(summary_prompt)] + messages[-1:])
                result = RouterModel.model_validate({'value': llm_output.content})
            else:
                result = structured_llm.invoke([router_system_message(summary_prompt)] + messages[-1:])

                if result is None:
                    raise ValueError("LLM returned None!")
        except (KeyError, ValidationError, ValueError, TypeError) as e:
            logger.warning(f"Unexpected router output: {e}. Falling back to 'pandas_agent'.")
            result = RouterModel(value="pandas_agent")

        if result.value == 'ml_agent':
            return 'ml_agent'
        else:
            return 'pandas_agent'  # we forward 'default' -> 'pandas_agent'

    # Sqlite checkpointer
    sqlite_db_path = SQLITE_CHECKPOINTER_DB_PATH.as_posix()
    os.makedirs(os.path.dirname(sqlite_db_path), exist_ok=True)
    conn = sqlite3.connect(sqlite_db_path, check_same_thread=False)
    memory = SqliteSaver(conn)

    # Graph
    builder = StateGraph(State)

    # Define nodes: these do the work
    builder.add_node("conversation", conversation_node)
    builder.add_node("pandas_agent", pandas_agent_node)
    builder.add_node("ml_agent", ml_agent_node)

    # Define edges: these determine how the control flow moves
    builder.add_edge(START, "conversation")
    builder.add_conditional_edges(
        "conversation",
        router,
    )
    builder.add_edge("pandas_agent", END)
    builder.add_edge("ml_agent", END)

    aconn = await aiosqlite.connect(sqlite_db_path)
    saver = AsyncSqliteSaver(aconn)
    
    graph = builder.compile(checkpointer=saver)

    if return_memory:
        return graph, memory
    
    return graph
