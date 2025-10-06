import os
import time as ttime

from prefect import flow, get_run_logger, task
from tiled.client import from_profile, from_uri
from utils import get_tiled_client


@task(retries=2, retry_delay_seconds=10)
def read_all_streams(uid, beamline_acronym):
    logger = get_run_logger()
    tiled_server_type = os.environ.get("TILED_SERVER_TYPE")
    if tiled_server_type == "facility":
        tiled_client = get_tiled_client()
        run = tiled_client["raw"][uid]
    elif tiled_server_type == "local":
        tiled_client = from_uri("http://localhost:8000")
        run = tiled_client[uid]
    logger.info(f"Validating uid {run.metadata['start']['uid']}")
    start_time = ttime.monotonic()
    for stream in run["streams"]:
        logger.info(f"{stream}:")
        stream_start_time = ttime.monotonic()
        stream_data = run[stream].read()
        stream_elapsed_time = ttime.monotonic() - stream_start_time
        logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
        logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"{elapsed_time = }")


@flow
def data_validation(uid):
    read_all_streams(uid, beamline_acronym="hex")
