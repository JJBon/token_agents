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
from scrapegraphai.nodes.base_node import BaseNode
import json

class EndNode(BaseNode):
    """
    No-op terminal node so ConditionalNode's FALSE branch points to a real node.
    """
    def __init__(
        self,
        node_name: str = "__END__",
        input: str = "",
        output=None,
        node_config=None,
    ):
        super().__init__(
            node_name=node_name,
            input=input or "",
            output=output or [],
            node_config=node_config or {},
            node_type="node",   # <-- MUST be 'node' (not 'end')
        )

    def execute(self, state: dict) -> dict:
        # Do nothing; just terminate the run cleanly.
        return {}


class DeduplicateNode(BaseNode):
    def __init__(self, input="answer", output=["answer"], node_name="Deduplicate"):
        super().__init__(node_name, "node", input, output, 1, node_config={})

    def _norm_title(self, t):
        t = (t or "").lower()
        t = re.sub(r'\s+', ' ', t).strip()
        return t

    def _norm_link(self, url):
        if not url: return ""
        url = url.strip()
        # strip tracking params
        url = url.split("#")[0]
        base, *qs = url.split("?")
        if qs:
            # keep only stable params if you want, else drop all
            url = base
        return url.lower()

    def execute(self, state):
        payload = state.get("answer", "")
        try:
            data = json.loads(payload)
        except Exception:
            return state  # if not JSON, don't break the run

        for token, obj in list(data.items()):
            items = obj.get("news", [])
            seen = set()
            deduped = []
            for it in items:
                key = (self._norm_title(it.get("title","")),
                       self._norm_link(it.get("link","")))
                if key in seen: 
                    continue
                seen.add(key)
                deduped.append(it)
            obj["news"] = deduped

        state["answer"] = json.dumps(data, ensure_ascii=False)
        return state
    
class ValidateQuotaNode(BaseNode):
    def __init__(self, input="answer", output=["answer"], node_name="ValidateQuota"):
        super().__init__(node_name, "node", input, output, 1, node_config={})
        self.tokens = ["ethereum","bitcoin","solana","ripple","dogecoin","cardano","polkadot","other"]

    def execute(self, state):
        import json
        raw = state.get("answer","")
        try:
            data = json.loads(raw)
        except Exception:
            state["__valid__"] = False
            state["__reason__"] = "invalid_json"
            return state

        total = 0
        per = {}
        hard_fail = False
        for t in self.tokens:
            n = len(data.get(t,{}).get("news",[])) if isinstance(data.get(t),dict) else 0
            per[t] = n
            total += n

        # hard limits per listed token (other excluded)
        listed = ["ethereum","bitcoin","solana","ripple","dogecoin","cardano","polkadot"]
        bounds_fail = any(not (5 <= per.get(t,0) <= 6) for t in listed)

        state["__valid__"] = (total == 40 and not bounds_fail)
        state["__reason__"] = None if state["__valid__"] else {
            "total": total, "per_token": per, "bounds_fail": bounds_fail
        }
        return state

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

        end_node = EndNode(node_name="__END__")   

        dedup_node = DeduplicateNode()


        
        fetch_node = FetchNode(
            input="url | local_dir",
            output=["doc"],
            node_config={
                "llm_model": self.llm_model,
                "force": True,
                "cut": False,
                "headless": True,
                # IMPORTANT: remove browser_base completely so ChromiumLoader is used
                # "browser_base": None,
                "storage_state": self.config.get("storage_state"),
            }
            # node_config={
            #     "llm_model": self.llm_model,
            #     "force": self.config.get("force", False),
            #     "cut": self.config.get("cut", True),
            #     "loader_kwargs": self.config.get("loader_kwargs", {}),
            #     "browser_base": self.config.get("browser_base"),
            #     "scrape_do": self.config.get("scrape_do"),
            #     "storage_state": self.config.get("storage_state"),
            # },
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
            # name the regen node so the conditional can target it
            regen_node = GenerateAnswerNode(
                input="user_prompt & answer",
                output=["answer"],
                node_name="RegenerateAnswerNode",
                node_config={
                    "llm_model": self.llm_model,
                    "additional_info": REGEN_ADDITIONAL_INFO,
                    "schema": self.schema,
                },
            )

            cond_node = ConditionalNode(
                input="answer",
                output=["answer"],
                node_name="CheckAnswer",
                node_config={
                    "key_name": "answer",
                    "condition": 'not answer or answer=="NA"',
                    "true_node_name": "RegenerateAnswerNode",  # go to regen on TRUE
                    "false_node_name": "__END__",              # end graph on FALSE
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
                "nodes": [fetch_node, parse_node, reasoning_node, generate_answer_node, dedup_node],
                "edges": [
                    (fetch_node, parse_node),
                    (parse_node, reasoning_node),
                    (reasoning_node, generate_answer_node),
                    (generate_answer_node, dedup_node),
                ],
            },
            (True, True, False): {
                "nodes": [fetch_node, reasoning_node, generate_answer_node, dedup_node],
                "edges": [
                    (fetch_node, reasoning_node),
                    (reasoning_node, generate_answer_node),
                    (generate_answer_node, dedup_node),
                ],
            },
            (True, False, False): {
                "nodes": [fetch_node, generate_answer_node, dedup_node],
                "edges": [
                    (fetch_node, generate_answer_node),
                    (generate_answer_node, dedup_node),
                ],
            },
            (False, False, False): {
                "nodes": [fetch_node, parse_node, generate_answer_node, dedup_node],
                "edges": [
                    (fetch_node, parse_node),
                    (parse_node, generate_answer_node),
                    (generate_answer_node, dedup_node),
                ],
            },
            (False, True, True): {
                "nodes": [fetch_node, parse_node, reasoning_node, generate_answer_node, dedup_node, cond_node, regen_node, end_node],
                "edges": [
                    (fetch_node, parse_node),
                    (parse_node, reasoning_node),
                    (reasoning_node, generate_answer_node),
                    (generate_answer_node, dedup_node),
                    (dedup_node, cond_node),
                    (cond_node, regen_node),
                    (cond_node, end_node),
                ],
            },
            (True, True, True): {
                "nodes": [fetch_node, reasoning_node, generate_answer_node, dedup_node, cond_node, regen_node, end_node],
                "edges": [
                    (fetch_node, reasoning_node),
                    (reasoning_node, generate_answer_node),
                    (generate_answer_node, dedup_node),
                    (dedup_node, cond_node),
                    (cond_node, regen_node),
                    (cond_node, end_node),
                ],
            },
            (True, False, True): {
                "nodes": [fetch_node, generate_answer_node, dedup_node, cond_node, regen_node, end_node],
                "edges": [
                    (fetch_node, generate_answer_node),
                    (generate_answer_node, dedup_node),
                    (dedup_node, cond_node),
                    (cond_node, regen_node),
                    (cond_node, end_node),
                ],
            },
            (False, False, True): {
                "nodes": [fetch_node, parse_node, generate_answer_node, dedup_node, cond_node, regen_node, end_node],
                "edges": [
                    (fetch_node, parse_node),
                    (parse_node, generate_answer_node),
                    (generate_answer_node, dedup_node),
                    (dedup_node, cond_node),
                    (cond_node, regen_node),
                    (cond_node, end_node),
                ],
            }
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
    0) Do slow queries, avoid rate limits. Wait at least 10 seconds before requests.
    1) From the page, fetch EXACTLY 40 trending crypto news items visible on the site.
    2) For each item, click through to the article and read briefly.
    3) Write a 1-2 sentence insight that hints bullish/bearish/neutral.
    4) Group insights by token.

    Output (JSON only, no prose):
    {
    "ethereum": { "news": [ { "title": "", "insights": "", "link": "" } ] },
    "bitcoin":  { "news": [] },
    "solana":   { "news": [] },
    "ripple":   { "news": [] },
    "dogecoin": { "news": [] },
    "cardano":  { "news": [] },
    "polkadot": { "news": [] },
    "other":    { "news": [] }  // optional backfill bucket
    }

    Rules:
    - Group by tokens: bitcoin/btc, ethereum/eth, solana/sol, ripple/xrp, dogecoin/doge, cardano/ada, polkadot/dot.
    - Prefer the 7 listed tokens. Use "other" only if a listed token has <5 valid items available.
    - Aim for UNIFORM distribution: 5-6 items per listed token.
    * Hard limits per listed token: MIN 5, MAX 6.
    * If a token has <5 items available, fill the shortfall in "other".
    - EXACTLY 40 total items across all tokens combined (including "other" if used).
    - Keep insights ≤ 280 chars.
    - Return valid JSON, no markdown, no commentary.

    Validation (before returning):
    - Count items per token. If any listed token has <5, pull surplus from tokens with >6 where possible.
    - If still <5 after rebalancing, place additional items in "other".
    - Ensure total == 40 and all listed tokens are within 5-6 items.
    """
    graph = SmartScraperGraph(
            prompt=PROMPT,
            source="https://cryptopanic.com/news?filter=hot",
            config={ "llm":
                     {
                        "model": "anthropic.claude-3-haiku-20240307-v1:0",
                        "temperature":0,
                        "max_tokens": 40000,
                        "model_tokens": 40000
                    }, 
                "verbose": True,
                "html_mode": True,
                "reattempt": True
            }
        )
    graph.run()