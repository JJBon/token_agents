from langfuse import get_client

lf = get_client()

# Create dataset
dataset = lf.create_dataset(name="Fetch Bitcoin Weekly Data")

# Add dataset item with full expected structure
lf.create_dataset_item(
    dataset_name=dataset.name,
    input="fetch data for Bitcoin  (token_day__token_name == 'Bitcoin') . Aggregate results by week , perform analysis",
    expected_output={
        "expected_tools": [
            {"name": "fetch_metrics", "args": {}},
            {
                "name": "search_dimension_values",
                "args": {
                    "dimension": "token_day__coin_name",
                    "query": "Bitcoin",
                    "max_results": 10
                }
            },
            {
                "name": "create_query",
                "args": {
                    "metrics": ["average_price_usd", "average_market_cap_usd"],
                    "group_by": [
                        {"type": "time", "dimension": "metric_time", "aggregation": "week"}
                    ],
                    "where": {
                        "conditions": [
                            {
                                "type": "dimension",
                                "dimension": "token_day__coin_name",
                                "operator": "=",
                                "value": "Bitcoin"
                            }
                        ],
                        "logic": "AND"
                    },
                    "order_by": ["metric_time"]
                }
            },
            {
                "name": "fetch_query_result",
                "args": {
                    "metrics": ["average_price_usd", "average_market_cap_usd"],
                    "group_by": [
                        {"type": "time", "dimension": "metric_time", "aggregation": "week"}
                    ],
                    "where": {
                        "conditions": [
                            {
                                "type": "dimension",
                                "dimension": "token_day__coin_name",
                                "operator": "=",
                                "value": "Bitcoin"
                            }
                        ],
                        "logic": "AND"
                    },
                    "order_by": ["metric_time"]
                }
            }
        ],
        "expected_keywords": ["Bitcoin"],
        "expected_flow": [
            "fetch_metrics",
            "search_dimension_values",
            "create_query",
            "fetch_query_result"
        ]
    }
)

print(f"Dataset created with ID: {dataset.id}")
