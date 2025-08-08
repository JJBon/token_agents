"""
SmartScraperGraph Module
"""

from typing import Optional, Type

from pydantic import BaseModel

from scrapegraphai.nodes.fetch_node import FetchNode        
from scrapegraphai.nodes.generate_answer_node import GenerateAnswerNode
from scrapegraphai.nodes.parse_node import ParseNode
from scrapegraphai.nodes.generate_answer_node import GenerateAnswerNode
from scrapegraphai.nodes.reasoning_node import ReasoningNode
from scrapegraphai.nodes.conditional_node  import ConditionalNode
from scrapegraphai.prompts.generate_answer_node_prompts import REGEN_ADDITIONAL_INFO
from agents.news_agent.AbstractGraph import AbstractGraph
from scrapegraphai.graphs.base_graph import BaseGraph 
import boto3
from langchain_aws import ChatBedrockConverse

class SmartScraperGraph(AbstractGraph):
    """
    SmartScraper is a scraping pipeline that automates the process of
    extracting information from web pages
    using a natural language model to interpret and answer prompts.

    Attributes:
        prompt (str): The prompt for the graph.
        source (str): The source of the graph.
        config (dict): Configuration parameters for the graph.
        schema (BaseModel): The schema for the graph output.
        llm_model: An instance of a language model client, configured for generating answers.
        embedder_model: An instance of an embedding model client,
        configured for generating embeddings.
        verbose (bool): A flag indicating whether to show print statements during execution.
        headless (bool): A flag indicating whether to run the graph in headless mode.

    Args:
        prompt (str): The prompt for the graph.
        source (str): The source of the graph.
        config (dict): Configuration parameters for the graph.
        schema (BaseModel): The schema for the graph output.

    Example:
        >>> smart_scraper = SmartScraperGraph(
        ...     "List me all the attractions in Chioggia.",
        ...     "https://en.wikipedia.org/wiki/Chioggia",
        ...     {"llm": {"model": "openai/gpt-3.5-turbo"}}
        ... )
        >>> result = smart_scraper.run()
        )
    """

    def __init__(
        self,
        prompt: str,
        source: str,
        config: dict,
        schema: Optional[Type[BaseModel]] = None,
    ):
        super().__init__(prompt, config, source, schema)

        self.input_key = "url" if source.startswith("http") else "local_dir"

        # for detailed logging of the SmartScraper API set it to True
        self.verbose = config.get("verbose", False)

    def _create_graph(self) -> BaseGraph:
        """
        Creates the graph of nodes representing the workflow for web scraping.

        Returns:
            BaseGraph: A graph instance representing the web scraping workflow.
        """


      

        # if self.llm_model == "scrapegraphai/smart-scraper":
        #     try:
        #         from scrapegraph_py import Client
        #         from scrapegraph_py.logger import sgai_logger
        #     except ImportError:
        #         raise ImportError(
        #             "scrapegraph_py is not installed. Please install it using 'pip install scrapegraph-py'."
        #         )

        #     sgai_logger.set_logging(level="INFO")

        #     # Initialize the client with explicit API key
        #     sgai_client = Client(api_key=self.config.get("api_key"))

        #     # SmartScraper request
        #     response = sgai_client.smartscraper(
        #         website_url=self.source,
        #         user_prompt=self.prompt,
        #     )

        #     # Print the response
        #     print(f"Request ID: {response['request_id']}")
        #     print(f"Result: {response['result']}")

        #     sgai_client.close()

        #     return response

        fetch_node = FetchNode(
            input="url | local_dir",
            output=["doc"],
            node_config={
                "llm_model": self.llm_model,
                "force": self.config.get("force", False),
                "cut": self.config.get("cut", True),
                "loader_kwargs": self.config.get("loader_kwargs", {}),
                "browser_base": self.config.get("browser_base"),
                "scrape_do": self.config.get("scrape_do"),
                "storage_state": self.config.get("storage_state"),
            },
        )
        parse_node = ParseNode(
            input="doc",
            output=["parsed_doc"],
            node_config={"llm_model": self.llm_model, "chunk_size": 18000 } #self.model_token},
        )

        generate_answer_node = GenerateAnswerNode(
            input="user_prompt & (relevant_chunks | parsed_doc | doc)",
            output=["answer"],
            node_config={
                "llm_model": self.llm_model,
                "additional_info": self.config.get("additional_info"),
                "schema": self.schema,
            },
        )

        cond_node = None
        regen_node = None
        if self.config.get("reattempt") is True:
            cond_node = ConditionalNode(
                input="answer",
                output=["answer"],
                node_name="ConditionalNode",
                node_config={
                    "key_name": "answer",
                    "condition": 'not answer or answer=="NA"',
                },
            )
            regen_node = GenerateAnswerNode(
                input="user_prompt & answer",
                output=["answer"],
                node_config={
                    "llm_model": self.llm_model,
                    "additional_info": REGEN_ADDITIONAL_INFO,
                    "schema": self.schema,
                },
            )

        if self.config.get("html_mode") is False:
            parse_node = ParseNode(
                input="doc",
                output=["parsed_doc"],
                node_config={
                    "llm_model": self.llm_model,
                    "chunk_size": self.model_token,
                },
            )

        reasoning_node = None
        if self.config.get("reasoning"):
            reasoning_node = ReasoningNode(
                input="user_prompt & (relevant_chunks | parsed_doc | doc)",
                output=["answer"],
                node_config={
                    "llm_model": self.llm_model,
                    "additional_info": self.config.get("additional_info"),
                    "schema": self.schema,
                },
            )

        # Define the graph variation configurations
        # (html_mode, reasoning, reattempt)
        graph_variation_config = {
            (False, True, False): {
                "nodes": [fetch_node, parse_node, reasoning_node, generate_answer_node],
                "edges": [
                    (fetch_node, parse_node),
                    (parse_node, reasoning_node),
                    (reasoning_node, generate_answer_node),
                ],
            },
            (True, True, False): {
                "nodes": [fetch_node, reasoning_node, generate_answer_node],
                "edges": [
                    (fetch_node, reasoning_node),
                    (reasoning_node, generate_answer_node),
                ],
            },
            (True, False, False): {
                "nodes": [fetch_node, generate_answer_node],
                "edges": [(fetch_node, generate_answer_node)],
            },
            (False, False, False): {
                "nodes": [fetch_node, parse_node, generate_answer_node],
                "edges": [(fetch_node, parse_node), (parse_node, generate_answer_node)],
            },
            (False, True, True): {
                "nodes": [
                    fetch_node,
                    parse_node,
                    reasoning_node,
                    generate_answer_node,
                    cond_node,
                    regen_node,
                ],
                "edges": [
                    (fetch_node, parse_node),
                    (parse_node, reasoning_node),
                    (reasoning_node, generate_answer_node),
                    (generate_answer_node, cond_node),
                    (cond_node, regen_node),
                    (cond_node, None),
                ],
            },
            (True, True, True): {
                "nodes": [
                    fetch_node,
                    reasoning_node,
                    generate_answer_node,
                    cond_node,
                    regen_node,
                ],
                "edges": [
                    (fetch_node, reasoning_node),
                    (reasoning_node, generate_answer_node),
                    (generate_answer_node, cond_node),
                    (cond_node, regen_node),
                    (cond_node, None),
                ],
            },
            (True, False, True): {
                "nodes": [fetch_node, generate_answer_node, cond_node, regen_node],
                "edges": [
                    (fetch_node, generate_answer_node),
                    (generate_answer_node, cond_node),
                    (cond_node, regen_node),
                    (cond_node, None),
                ],
            },
            (False, False, True): {
                "nodes": [
                    fetch_node,
                    parse_node,
                    generate_answer_node,
                    cond_node,
                    regen_node,
                ],
                "edges": [
                    (fetch_node, parse_node),
                    (parse_node, generate_answer_node),
                    (generate_answer_node, cond_node),
                    (cond_node, regen_node),
                    (cond_node, None),
                ],
            },
        }

        # Get the current conditions
        html_mode = self.config.get("html_mode", False)
        reasoning = self.config.get("reasoning", False)
        reattempt = self.config.get("reattempt", False)

        # Retrieve the appropriate graph configuration
        config = graph_variation_config.get((html_mode, reasoning, reattempt))

        if config:
            return BaseGraph(
                nodes=config["nodes"],
                edges=config["edges"],
                entry_point=fetch_node,
                graph_name=self.__class__.__name__,
            )

        # Default return if no conditions match
        return BaseGraph(
            nodes=[fetch_node, parse_node, generate_answer_node],
            edges=[(fetch_node, parse_node), (parse_node, generate_answer_node)],
            entry_point=fetch_node,
            graph_name=self.__class__.__name__,
        )

    def run(self) -> str:
        """
        Executes the scraping process and returns the answer to the prompt.

        Returns:
            str: The answer to the prompt.
        """

        inputs = {"user_prompt": self.prompt, self.input_key: self.source}
        self.final_state, self.execution_info = self.graph.execute(inputs)

        return self.final_state.get("answer", "No answer found.")
    
if __name__ == "__main__":
    bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
    llm_model = ChatBedrockConverse(
            model="anthropic.claude-3-haiku-20240307-v1:0",
            provider="anthropic",
            temperature=0,
            client=bedrock
        )
    PROMPT = """
        You are a careful news researcher.

        Task:
        0) Do slow queries, avoid rate limits. Wait at least 10 seconds before requests
        1) From the page, fetch EXACTLY 20 trending crypto news items visible on the site.
        2) For each item, click through to the article and read briefly.
        3) Write a 1-2 sentence insight that hints bullish/bearish/neutral.
        4) Group insights by token 

        Output (JSON only, no prose):
        {
        "ethereum": { "news": [ {"title": "", "insights": "", "link": ""} ] },
        "bitcoin": { "news": [] },
        "solana":  { "news": [] },
        "ripple":  { "news": [] },
        "dogecoin":{ "news": [] },
        "cardano": { "news": [] },
        "polkadot":{ "news": [] }
        }

        Rules:
        - Group by tokens: bitcoin/btc, ethereum/eth, solana/sol, ripple/xrp, dogecoin/doge, cardano/ada, polkadot/dot.
        - If unsure, omit the token.
        - Max 20 total items across all tokens.
        - Keep insights short (≤ 280 chars).
        - Return valid JSON, no markdown, no commentary.
    """
    graph = SmartScraperGraph(
            prompt=PROMPT,
            source="https://cryptopanic.com",
            config={ "llm":
                     {
                        "model": "anthropic.claude-3-haiku-20240307-v1:0",
                        "temperature":0,
                        "max_tokens": 20000,
                        "model_tokens": 20000
                    }, 
                "verbose": True,
                "html_mode": True,
                "reattempt": True
            }
        )
    graph.run()