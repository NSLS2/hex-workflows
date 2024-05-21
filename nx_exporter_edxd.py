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

    start_doc = run.metadata["start"]

    if start_doc.get("theta") is not None:
        file_prefix = None
    else:
        file_prefix = "scan_{start[scan_id]:05d}_{date.month:02d}_{date.day:02d}_{date.year:04d}.nxs"

    # TODO: commissioning will need to be replaced by cycle after testing
    # /nsls2/data/hex/proposals/commissioning/pass-315258/exported_data/
    # NOTE: exported_data is folder for automated exporting
    if start_doc.get("export_dir") is not None:
        export_dir = None
    else:
        export_dir = f"/nsls2/data/hex/proposals/{start_doc.get('cycle')}/{start_doc.get('data_session')}/"

    nx_file_path = nx_export(run, det_name, file_prefix=file_prefix, export_dir=export_dir)
    logger.info(f"Exported file: {nx_file_path}")
    return nx_file_path


# Make the Prefect Flow.
# A separate command is needed to register it with the Prefect server.
@flow
def export_edxd_flow(ref, det_name="GeRM"):
    logger = get_run_logger()
    logger.info(f"tiled: {tiled.__version__}")
    logger.info(f"profiles: {tiled.profiles.list_profiles()['nsls2']}")
    export(ref, det_name=det_name)
