from tiled.client import from_profile
from prefect.blocks.system import Secret

import os

LOCATION = "hex"

def get_tiled_client():
    os.environ["TILED_API_KEY"] = Secret.load(f"tiled-{LOCATION}-api-key", _sync=True).get()
    tiled_client = from_profile("nsls2")[LOCATION]
    os.environ.pop("TILED_API_KEY")
    return tiled_client
