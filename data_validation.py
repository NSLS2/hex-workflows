from prefect import task, flow, get_run_logger
import time as ttime
from tiled.client import from_profile


@task(retries=2, retry_delay_seconds=10)
def read_all_streams(uid, beamline_acronym):
    logger = get_run_logger()
    tiled_client = from_profile("nsls2", username=None)
    run = tiled_client[beamline_acronym]["raw"][uid]
    logger.info(f"Validating uid {run.start['uid']}")
    start_time = ttime.monotonic()
    for stream in run:
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

