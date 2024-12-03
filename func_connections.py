from dydx_v4_client.indexer.rest.indexer_client import IndexerClient
from dydx_v4_client.network import LOCAL


# Connect to DYDX CompositeClient
async def connect_to_dydx():
    # Create client
    client = IndexerClient(LOCAL.rest_indexer)
    print(client)

    # Return Client
    return client
