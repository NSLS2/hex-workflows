import os
from prefect import flow, get_run_logger, task

from data_validation import data_validation
from nx_exporter_edxd import export_edxd_flow
from nx_exporter_tomo import export_tomo_flow
from tiled.client import from_uri
from dotenv import load_dotenv


def get_api_key_from_env(api_key=None):
    logger = get_run_logger()
    if not api_key:
        try:
            with open("/srv/container.secret", "r") as secrets:
                load_dotenv(stream=secrets)
        except Exception:
            logger.exception("Exception while getting Tiled API key")
        finally:
            api_key = os.environ["TILED_API_KEY"]
    return api_key


@task
def get_run(uid, api_key=None):
    tiled_client = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    run = tiled_client["hex"]["raw"][uid]
    return run


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow(log_prints=True)
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]
    print(f"{uid = }")
    api_key = get_api_key_from_env(api_key=None)
    run = get_run(uid, api_key=api_key)
    start_doc = run.metadata["start"]

    exit_status = run.stop.get("exit_status")
    if exit_status == "success":
        scan_type = start_doc.get("tomo_scanning_mode")
        print(f"{scan_type = }")

        if scan_type in ["tomo_dark_flat", "tomo_flyscan"]:
            print("Running export_tomo_flow")
            export_tomo_flow(uid, api_key=api_key)
        elif scan_type == "edxd":
            print("Running export_edxd_flow")
            export_edxd_flow(uid, api_key=api_key)
        else:
            print("Unknown tomo scanning mode. Not exporting.")

        # Disabling until validation fixed
        # data_validation(uid)
        log_completion()
    else:
        print(f"Not running flow. {exit_status = }")
