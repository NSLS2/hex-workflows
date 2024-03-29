from prefect import flow, get_run_logger, task

from data_validation import data_validation
# from nx_exporter_edxd import export_flow as export_edxd_flow
from nx_exporter_tomo import export_tomo_flow


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]
    # export_edxd_flow(uid)
    export_tomo_flow(uid)
    # TODO: revert it after switching back from local tiled to the facility one:
    # data_validation(uid)
    log_completion()
