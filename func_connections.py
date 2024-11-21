'''
Connect to DYDX Client
'''

from v4_client_py.clients import IndexerClient
from v4_client_py.clients.constants import Network


# Connect to DYDX CompositeClient
def connect_to_dydx():

    # Create client
    client = IndexerClient(
    config=Network.config_network().indexer_config,
   )
    print(client)

    # Return Client
    return client