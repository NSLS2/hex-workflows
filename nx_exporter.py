import tiled
from hextools.germ.export import nx_export
from prefect import flow, get_run_logger, task
from tiled.client import from_profile

tiled_client = from_profile("nsls2")["hex"]
tiled_client_hex = tiled_client["raw"]


@task
def export(ref, det_name="GeRM"):
    """
    Performs export of the data in the NX format.

    Parameters
    ----------
    ref : str
        This is the reference to the BlueskyRun to be exported. It can be
        a partial uid, a full uid, a scan_id, or an index (e.g. -1).
    det_name : str
        The detector name.

    Returns
    -------
    nx_file_path : string
        The path to the exported file.
    """
    logger = get_run_logger()

    # Get the BlueskyRun from Tiled.
    run = tiled_client_hex[ref]

    nx_file_path = nx_export(run, det_name)
    logger.info(f"Exported file: {nx_file_path}")
    return nx_file_path


# Make the Prefect Flow.
# A separate command is needed to register it with the Prefect server.
@flow
def export_flow(ref, det_name="GeRM"):
    logger = get_run_logger()
    logger.info(f"tiled: {tiled.__version__}")
    logger.info(f"profiles: {tiled.profiles.list_profiles()['nsls2']}")
    export(ref, det_name=det_name)
