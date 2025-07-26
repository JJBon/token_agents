import os, asyncio
from fastmcp import Client
from dbt_semantic_layer_mcp_server import CreateQueryInput,GroupByField,TimeAggregation,WhereCondition,WhereCondition,Logic,DimensionType,FilterField

async def test_stdio():
    client = Client("dbt_semantic_layer_mcp_server.py")
    client.transport.env = {
        "DBT_PROJECT_PATH": os.environ["DBT_PROJECT_PATH"],
        "DBT_PROFILES_DIR": os.environ["DBT_PROFILES_DIR"],
        "DBT_TARGET": os.environ.get("DBT_TARGET", "dev-mcp-server"),
    }

    async with client:
        await client.ping()
        tools = await client.list_tools()
        print("Tools:", [t.name for t in tools], "\n")

        for t in tools:
            print(f"Tool: {t.name}")
            print(f"  Description: {t.description}")
            print(f"  Input Schema: {t.inputSchema}")
            if hasattr(t, 'outputSchema'):
                print(f"  Output Schema: {t.outputSchema}")
            print("-" * 60)

        # 1️⃣ fetch_metrics
        res = await client.call_tool("fetch_metrics", {})
        metrics = res.structured_content.get("metrics", [])
        print(f"\nFetched {len(metrics)} metrics.")
        if not metrics:
            return

        m = metrics[0]
        print(f"\nUsing metric: {m['name']}")

        # 2️⃣ Build full query with "where" clause
        
        payload = CreateQueryInput(
            metrics=["average_price_usd", "market_cap_growth_rate"],
            group_by=[
                GroupByField(type=DimensionType.TIME, dimension="metric_time", aggregation=TimeAggregation.week),
                GroupByField(type=DimensionType.DIMENSION, dimension="token_day__coin_name")
            ],
            order_by=["-market_cap_growth_rate"],
            limit=10,
            where=WhereCondition(
                conditions=[
                    FilterField(
                        type=DimensionType.TIME,
                        dimension="metric_time",
                        aggregation=TimeAggregation.week,
                        operator="<=",
                        value="current_date"
                    ),
                    WhereCondition(
                        conditions=[
                            FilterField(
                                type=DimensionType.DIMENSION,
                                dimension="token_day__market_cap_usd_bucket",
                                operator="=",
                                value="Very Low Cap"
                            ),
                            FilterField(
                                type=DimensionType.DIMENSION,
                                dimension="token_day__market_cap_usd_bucket",
                                operator="=",
                                value="Low Cap"
                            )
                        ],
                        logic=Logic.OR
                    )
                ],
                logic=Logic.AND
            )
        )


        r2 = await client.call_tool("create_query", {"input_data": payload.dict()})
        print("\ncreate_query returned structured cont∫ent:")
        print(r2.structured_content)
        q = r2.structured_content.get("query")
        print("q is: " , q)
        if r2.structured_content.get("status") == "ERROR":
            print("❌ create_query failed:", r2.structured_content.get("error"))
            return
        print("\n✅ Created query:", q)

        #print(q["error"])

        # 4️⃣ fetch_query_result
        r3 = await client.call_tool("fetch_query_result", {"input_data": payload})
        print(r3.structured_content)
        print("\nfetch_query_result returned:")

asyncio.run(test_stdio())
