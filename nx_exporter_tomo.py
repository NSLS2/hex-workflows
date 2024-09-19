import os
from pathlib import Path

import h5py
import numpy as np
from prefect import flow, get_run_logger, task
from tiled.client import from_profile, from_uri
from tiled.client.utils import get_asset_filepaths


def get_filepath_from_run(run, stream_name):
    entry = run[stream_name]["external"].values().last()
    filepath = get_asset_filepaths(entry)[0]
    if not filepath.is_file():
        msg = f"{filepath!r} does not exist!"
        raise RuntimeError(msg)
    return filepath


def get_dtype(value):
    if isinstance(value, str):
        return h5py.special_dtype(vlen=str)
    if isinstance(value, float):
        return np.float32
    if isinstance(value, int):
        return np.int32
    return type(value)


@task
def export_tomo(run, export_dir=None):
    """Function to export bluesky run to a NeXus file.

    Parameters:
    -----------
    run : bluesky run
        the bluesky run to export to NeXus.
    export_dir : str (optional)
        the export directory for the resulting file.
    file_prefix : str (optional)
        the file prefix template for the resulting file.
    counter : int (optional)
        a counter to add to the file name.
    """

    start_doc = run.metadata["start"]

    det_filepaths = {}
    for stream in run:
        if "panda" in stream:
            panda_filepath = get_filepath_from_run(run, stream)
        elif "kinetix" in stream:
            det_filepaths[stream] = get_filepath_from_run(run, stream)

    # det_filepath = get_filepath_from_run(run, "kinetix-det1_stream")
    # panda_filepath = get_filepath_from_run(run, "panda1_stream")
    print(f"{det_filepaths = !r}\n{panda_filepath = !r}")

    if export_dir is None:
        export_dir = f"/nsls2/data/hex/proposals/{start_doc['cycle']}/{start_doc['data_session']}/tomography/scan_{start_doc['scan_id']:05d}/"
        # Create scan_00000 folder
        Path(export_dir).mkdir(parents=True, exist_ok=True)

    # need relative path of export_dir and panda_filepath for ExternalLink
    common_parent_dir = os.path.commonprefix([export_dir, panda_filepath])
    print(f"{common_parent_dir = }")

    rel_panda_filepath = Path(f"../../{Path(panda_filepath).relative_to(common_parent_dir)}")
    rel_det_filepaths = {}
    for stream_name, det_filepath in det_filepaths.items():
        rel_det_filepaths[stream_name] = Path(f"../../{Path(det_filepath).relative_to(common_parent_dir)}")

    print(f"{rel_panda_filepath = }")
    print(f"{rel_det_filepaths = }")

    filename = f"scan_{start_doc['scan_id']:05d}.nxs"

    nx_filepath = Path(export_dir) / Path(filename)
    print(f"{nx_filepath = }")

    if os.path.exists(nx_filepath):
        raise FileExistsError(f"{nx_filepath} exists. Not overwriting it")

    # Need 'a' instead of 'x' mode in order to change external link mode
    with h5py.File(nx_filepath, "a") as h5_file:
        entry_grp = h5_file.require_group("entry")
        data_grp = entry_grp.require_group("data")

        # current_metadata_grp = h5_file.require_group("entry/instrument/detector")
        # metadata = {"uid": start_doc["uid"]}
        # for key, value in metadata.items():
        #     if key not in current_metadata_grp:
        #         dtype = get_dtype(value)
        #         current_metadata_grp.create_dataset(key, data=value, dtype=dtype)

        # External links:
        # multiple kinetix dets - entry/data/data becomes entry/data/kinetix-det1, entry/data/kinetix-det2, etc.
        if len(det_filepaths) > 1:
            for stream_name, det_filepath in det_filepaths.items():
                print(f"{stream_name} ExternalLink creation")
                print(f"{det_filepath = }")
                nxs_data_name = stream_name.split("_")[0]
                data_grp[nxs_data_name] = h5py.ExternalLink(
                    det_filepath.as_posix(),
                    f"entry/data/data"
                )
                # data_grp.move("data", nxs_data_name)
        else:
            for stream_name, det_filepath in det_filepaths.items():
                data_grp["data"] = h5py.ExternalLink(
                    det_filepath.as_posix(),
                    "entry/data/data",
                )
        data_grp["rotation_angle"] = h5py.ExternalLink(
            rel_panda_filepath.as_posix(),
            "Angle",
        )

        # data = run.primary["data"][f"{det_name}_image"].read()
        # frame_shape = data.shape[1:]
        # data_grp.create_dataset(
        #     "data",
        #     data=data,
        #     maxshape=(None, *frame_shape),
        #     chunks=(1, *frame_shape),
        #     dtype=data.dtype,
        # )
    return nx_filepath


@flow(log_prints=True)
def export_tomo_flow(ref):
    uid = ref
    logger = get_run_logger()
    tiled_server_type = os.environ.get("TILED_SERVER_TYPE")
    if tiled_server_type == "facility":
        tiled_client = from_profile("nsls2")
        run = tiled_client["hex"]["raw"][uid]
    elif tiled_server_type == "local":
        tiled_client = from_uri("http://localhost:8000")
        run = tiled_client[uid]

    fp = export_tomo(run)


# if __name__ == "__main__":
#     tiled_client = from_uri(
#         "http://localhost:8000",
#         api_key=os.getenv("TILED_API_KEY", ""),
#         include_data_sources=True,
#     )
#
#     # uid = "27d30985-ca8b-46c9-93fd-64ffa7e88ac2"
#
#     # Saved in legacy:
#     # uid = "a6dc898f-5087-4ae5-863b-5c9f8ae6d0ac"  # run on 2024-03-28 at ~6:30 pm, 360 deg scan, 1801 frames
#     # uid = "01babb57-30b6-40f9-a115-daed23e8cfea"  # run on 2024-03-28 at ~8:00 pm, 360 deg scan, 3601 frames
#
#     # Saved in proposals:
#     # uid = "a1451ea2-55c5-4d45-a4c1-efc0872e4355"  # run on 2024-03-28 at ~8:10 pm, 180 deg scan, 1801 frames
#     uid = "db2182bd-f6e9-41f4-ae3f-b4e8bd594eb0"  # run on 2024-03-29 at ~8:00 am, 360 deg scan, 3601 frames
#
#     run = tiled_client[uid]
#
#     nx_filepath = export_tomo(run, export_dir=None, file_prefix=None, counter=0)
#     print(f"{nx_filepath = }")
