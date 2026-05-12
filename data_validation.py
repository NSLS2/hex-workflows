import os
import time as ttime

from prefect import flow, get_run_logger, task
from tiled.client import from_uri
from dotenv import load_dotenv


def get_api_key_from_env():
    with open("/srv/container.secret", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    return api_key


@task(retries=2, retry_delay_seconds=10)
def get_run(uid, api_key=None):
    if not api_key:
        api_key = get_api_key_from_env()
    cl = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    run = cl[f"{BEAMLINE_OR_ENDSTATION}/raw"][uid]
    return run


@task
def read_stream(run, stream):
    return run[stream].read()


@flow
def data_validation(uid, api_key=None):
    logger = get_run_logger()
    run = get_run(uid, api_key)
    logger.info(f"Validating uid {run.metadata['start']['uid']}")
    start_time = ttime.monotonic()
    for stream in run["streams"]:
        logger.info(f"{stream}:")
        stream_start_time = ttime.monotonic()
        stream_data = read_stream(run, stream)
        stream_elapsed_time = ttime.monotonic() - stream_start_time
        logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
        logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"{elapsed_time = }")
