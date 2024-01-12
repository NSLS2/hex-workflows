from prefect import task, flow, get_run_logger
from data_validation import data_validation


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]
    data_validation(uid)
    log_completion()

