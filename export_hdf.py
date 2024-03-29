import datetime
import os
from pathlib import Path

import h5py
import numpy as np
from tiled.client import from_uri
from tiled.client.utils import get_asset_filepaths


def get_filepath_from_run(run, stream_name):
    entry = getattr(run, stream_name)["external"].values().last()
    filepath = get_asset_filepaths(entry)[0]
    if not filepath.is_file():
        msg = f"{filepath!r} does not exist!"
        raise RuntimeError(msg)
    return filepath


def export_tomo(run, export_dir=None, file_prefix=None, counter=0):
    """Function to export bluesky run to NeXus file

    Parameters:
    -----------
    run : bluesky run
        the bluesky run to export to NeXus.
    export_dir : str (optional)
        the export directory for the resulting file.
    file_prefix : str (optional)
        the file prefix template for the resulting file.
    """
    start_doc = run.start
    date = datetime.datetime.fromtimestamp(start_doc["time"])

    if export_dir is None:
        # export_dir = "/nsls2/data/hex/legacy/flyscan_tests/just_kinetix"
        export_dir = "/nsls2/data/hex/proposals/commissioning/pass-315051/tomography/bluesky_test/exported"

    if file_prefix is None:
        file_prefix = "{start[plan_name]}_{start[scan_id]}_{date.year:04d}-{date.month:02d}-{date.day:02d}-{counter:03d}.nxs"

    rendered_file_name = file_prefix.format(start=start_doc, date=date, counter=counter)

    nx_filepath = Path(export_dir) / Path(rendered_file_name)
    print(f"{nx_filepath = }")

    def get_dtype(value):
        if isinstance(value, str):
            return h5py.special_dtype(vlen=str)
        if isinstance(value, float):
            return np.float32
        if isinstance(value, int):
            return np.int32
        return type(value)

    det_filepath = get_filepath_from_run(run, "kinetix_standard_det_stream")
    panda_filepath = get_filepath_from_run(run, "panda_standard_det_stream")
    print(f"{det_filepath = !r}\n{panda_filepath = !r}")

    common_parent_dir = os.path.commonprefix([export_dir, det_filepath, panda_filepath])

    # rel_nx = nx_filepath.relative_to(common_parent_dir)
    rel_det_filepath = det_filepath.relative_to(common_parent_dir)
    rel_panda_filepath = panda_filepath.relative_to(common_parent_dir)

    with h5py.File(nx_filepath, "x") as h5_file:
        entry_grp = h5_file.require_group("entry")
        data_grp = entry_grp.require_group("data")

        # current_metadata_grp = h5_file.require_group("entry/instrument/detector")
        # metadata = {"uid": start_doc["uid"]}
        # for key, value in metadata.items():
        #     if key not in current_metadata_grp:
        #         dtype = get_dtype(value)
        #         current_metadata_grp.create_dataset(key, data=value, dtype=dtype)

        # External links:
        data_grp["data"] = h5py.ExternalLink(
            rel_det_filepath.as_posix(), "entry/data/data"
        )
        data_grp["rotation_angle"] = h5py.ExternalLink(
            rel_panda_filepath.as_posix(), "CALC2.OUT.Value"
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


if __name__ == "__main__":
    tiled_client = from_uri(
        "http://localhost:8000",
        api_key=os.getenv("TILED_API_KEY", ""),
        include_data_sources=True,
    )

    # uid = "27d30985-ca8b-46c9-93fd-64ffa7e88ac2"

    # Saved in legacy:
    # uid = "a6dc898f-5087-4ae5-863b-5c9f8ae6d0ac"  # run on 2024-03-28 at ~6:30 pm, 360 deg scan, 1801 frames
    # uid = "01babb57-30b6-40f9-a115-daed23e8cfea"  # run on 2024-03-28 at ~8:00 pm, 360 deg scan, 3601 frames

    # Saved in assets:
    uid = "a1451ea2-55c5-4d45-a4c1-efc0872e4355"  # run on 2024-03-28 at ~8:10 pm, 180 deg scan, 1801 frames

    run = tiled_client[uid]

    nx_filepath = export_tomo(run, export_dir=None, file_prefix=None, counter=0)
    print(f"{nx_filepath = }")
