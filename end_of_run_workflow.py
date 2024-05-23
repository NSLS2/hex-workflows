from prefect import flow, get_run_logger, task

from data_validation import data_validation
from nx_exporter_edxd import export_edxd_flow
from nx_exporter_tomo import export_tomo_flow
from tiled.client import from_profile


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]
    tiled_client = from_profile("nsls2")
    run = tiled_client["hex"]["raw"][uid]
    start_doc = run.metadata["start"]

    if start_doc["plan_name"] in ["tomo_flyscan"]:
        export_tomo_flow(uid)

    # if "germ" in [det.lower() for det in run.metadata['start']["detectors"]]:
    #     export_edxd_flow(uid)
    if start_doc["plan_name"] in ["sweep_motion"]:
        export_edxd_flow(uid)

    data_validation(uid)
    log_completion()
