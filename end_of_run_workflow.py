import os
import traceback

from prefect import flow, get_run_logger, task
from prefect.blocks.notifications import SlackWebhook
from prefect.context import FlowRunContext
from prefect.settings import PREFECT_UI_URL

from data_validation import data_validation, get_run, get_api_key_from_env
from nx_exporter_edxd import export_edxd_flow
from nx_exporter_tomo import export_tomo_flow

CATALOG_NAME = "hex"


def slack(func):
    """
    Send a message to mon-prefect and mon-prefect-hxm slack channels if the flow-run failed.
    Send a message to mon-prefect-hex slack channel with the flow-run status.
    Send a message to mon-bluesky slack channel if the bluesky-run failed.

    NOTE: the name of this inner function is the same as the real end_of_workflow() function because
    when the decorator is used, Prefect sees the name of this inner function as the name of
    the flow. To keep the naming of workflows consistent, the name of this inner function had to match the expected name.
    """

    def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
        flow_run_name = FlowRunContext.get().flow_run.dict().get("name")

        # Load slack credentials that are saved in Prefect.
        mon_prefect = SlackWebhook.load("mon-prefect")
        mon_bluesky = SlackWebhook.load("mon-bluesky")
        mon_prefect_hex = SlackWebhook.load("mon-prefect-hex")
        mon_prefect_hxm = SlackWebhook.load("mon-prefect-hxm")

        # Get the uid.
        uid = stop_doc["run_start"]

        # Get Tiled API key, if not set already
        if not api_key:
            api_key = get_api_key_from_env()

        # Get the scan_id.
        run = get_run(uid, api_key=api_key)
        scan_id = run.start["scan_id"]

        # Send a message to mon-bluesky if bluesky-run failed.
        if stop_doc.get("exit_status") == "fail":
            mon_bluesky.notify(
                f":bangbang: {CATALOG_NAME} bluesky-run failed. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```reason: {stop_doc.get('reason', 'none')}```"
            )

        try:
            result = func(stop_doc, api_key=api_key, dry_run=dry_run)

            # Send a message to mon-prefect-hex if flow-run is successful.
            message = f":white_check_mark: {CATALOG_NAME} flow-run successful. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}```"
            mon_prefect_hex.notify(message)
            return result
        except Exception as e:
            tb = traceback.format_exception_only(e)

            # Send a message to mon-prefect-hex, mon-prefect if flow-run failed.
            message = f":bangbang: {CATALOG_NAME} flow-run failed. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```{tb[-1]}```"
            mon_prefect.notify(message)
            mon_prefect_hex.notify(message)
            flow_run = FlowRunContext.get().flow_run
            # Add link to flow-run for the message to mon-prefect-hxm.
            program_message = (
                f":bangbang: {CATALOG_NAME} flow-run failed. <https://{PREFECT_UI_URL.value()}/flow-runs/"
                + f"flow-run/{flow_run.id}|the flow run link> (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```{tb[-1]}```"
            )
            mon_prefect_hxm.notify(program_message)
            raise

    return end_of_run_workflow
@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow(log_prints=True)
def end_of_run_workflow(stop_doc, api_key=None, dry_run=None):
    uid = stop_doc["run_start"]
    print(f"{uid = }")
    if not api_key:
        api_key = get_api_key_from_env(api_key=None)
    run = get_run(uid, api_key=api_key)
    start_doc = run.metadata["start"]

    exit_status = run.stop.get("exit_status")
    if exit_status == "success":
        scan_type = start_doc.get("tomo_scanning_mode")
        print(f"{scan_type = }")

        if scan_type in ["tomo_dark_flat", "tomo_flyscan"]:
            print("Running export_tomo_flow")
            export_tomo_flow(uid, api_key=api_key, dry_run=dry_run)
        elif scan_type == "edxd":
            print("Running export_edxd_flow")
            export_edxd_flow(uid, api_key=api_key, dry_run=dry_run)
        else:
            print("Unknown tomo scanning mode. Not exporting.")

        # Disabling until validation fixed
        # data_validation(uid)
        log_completion()
    else:
        print(f"Not running flow. {exit_status = }")
