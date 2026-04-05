import dlt
from dlt.sources.rest_api import rest_api_resources
from pendulum import now


def add_timestamp(data):
    data["timestamp"] = now().format("YYYY-MM-DD HH:mm")
    return data


@dlt.source(name="coingecko_api")
def coingecko_source():
    config = {
        "client": {"base_url": "https://api.coingecko.com/api/v3/simple"},
        "resources": [
            {
                "name": "price",
                "table_name": "hourly_market_data",
                "endpoint": {
                    "params": {
                        "vs_currencies": "usd",
                        "ids": "bitcoin,ethereum,solana",
                        "include_market_cap": "true",
                        "include_24hr_vol": "true",
                        "precision": 4
                    }
                }
            }
        ]
    }

    yield from rest_api_resources(config)


def load_crypto_market_data(event, lambda_context):
    pipeline = dlt.pipeline(
        pipeline_name="coingecko_api",
        destination="filesystem",
        dataset_name="coingecko"
    )

    source = coingecko_source()
    source.resources["price"].add_map(add_timestamp)

    pipeline.run(source)
