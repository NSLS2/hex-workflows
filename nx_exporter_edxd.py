import datetime
import os
from pathlib import Path
from shutil import copy2

import h5py
import numpy as np
import tiled
from prefect import flow, task
from tiled.client import from_profile
from tiled.client.utils import get_asset_filepaths

tiled_client = from_profile("nsls2")["hex"]
tiled_client_hex = tiled_client["raw"]

GERM_DETECTOR_KEYS = [
    "count_time",
    "gain",
    "shaping_time",
    "hv_bias",
    "voltage",
]

# motor info
# In [25]: [*run['baseline']['config']]
# Out[25]: ['kinetix-det1-drv-acquire_time',
#  'mca1_motors_fltr1d',  # Filter_1_Downstream
#  'mca1_motors_fltr3',  # Filter_3
#  'mca1_motors_fltr1u',  # Filter_1_Upstream
#  'mca1_motors_fltr2',  # Filter_2
#  'mca1_motors_slitb',  # Bottom
#  'mca1_motors_sliti',  # Inboard
#  'mca1_motors_slito',  # Outboard
#  'mca1_motors_slitt']  # Top

# In [15]: run.baseline['config']['mca1_motors_slito']['mca1_motors_slito'][:][0]
# Out[15]: 2.0001


def get_filepath_from_run(run, stream_name):
    entry = run[stream_name]["external"].values().last()
    filepath = get_asset_filepaths(entry)[0]
    if not filepath.is_file():
        msg = f"{filepath!r} does not exist!"
        raise RuntimeError(msg)
    return filepath


def get_det_copy_filepath(start_doc):
    det_copy_filepath = f"/nsls2/data/hex/proposals/{start_doc['cycle']}/{start_doc['data_session']}/edxd/raw_data/scan_{start_doc['scan_id']:05d}/"
    Path(det_copy_filepath).mkdir(parents=True, exist_ok=True)
    det_copy_filename = f"scan_{start_doc['scan_id']:05d}_{start_doc['uid']}.h5"
    full_det_filename = det_copy_filepath + det_copy_filename
    return full_det_filename


@task
def copy_file(run):
    start_doc = run.metadata["start"]
    # Get det data file in assets dir
    source = get_filepath_from_run(run, "primary")
    print(f"{source = }")
    # Get destination dir in proposal dir
    dest = get_det_copy_filepath(start_doc)
    print(f"Copying {source} to {dest}")
    copy2(source, dest)
    print("Done copying file")
    return dest


def get_detector_parameters_from_tiled(run, det_name=None, keys=None):
    """Auxiliary function to get detector parameters from tiled.

    Parameters:
    -----------
    run : bluesky run
        the bluesky run to get detector parameters for
    det_name : str
        ophyd detector name
    keys : dict
        the detector keys to get the values for to the returned dictionary

    Returns:
    --------
    detector_metadata : dict
        the dictionary with detector parameters
    """
    if det_name is None:
        msg = "The 'det_name' cannot be None"
        raise ValueError(msg)
    try:
        # make sure det_name is correct
        config = run.primary["config"][det_name]
    except KeyError as err:
        msg = f"{err} det_name is incorrect. Check ophyd device .name"
        raise ValueError(msg) from err
    if keys is None:
        keys = GERM_DETECTOR_KEYS
    group_key = f"{det_name.lower()}_detector"
    detector_metadata = {group_key: {}}
    for key in keys:
        detector_metadata[group_key][key] = config[f"{det_name}_{key}"][:][0]
    return detector_metadata


def get_motor_metadata(run):
    all_motors = {*()}
    for k in sorted(run.baseline['config']):
        if 'motor' in k:
           all_motors.add(k.split("_")[0])
    nested_motor_metadata = {motor_name: {} for motor_name in all_motors}
    for k in sorted(run.baseline['config']):
        if 'motor' in k:
            motor_name = k.split("_")[0]
            nested_motor_metadata[motor_name][k] = run.baseline['config'][k][k][:][0]

    return nested_motor_metadata

@task
def create_combined_file(run, det_name, copied_det_file):
    start_doc = run.start
    export_dir = f"/nsls2/data/hex/proposals/{start_doc['cycle']}/{start_doc['data_session']}/edxd/raw_data/scan_{start_doc['scan_id']:05d}/"
    date = datetime.datetime.fromtimestamp(start_doc["time"])

    if start_doc.get("theta") is not None:
        filename = f"scan_{start_doc['scan_id']:05d}_{start_doc['calibrant']}_{start_doc['theta']:.3f}deg_{date.month:02d}_{date.day:02d}_{date.year:04d}.h5"
    else:
        filename = f"scan_{start_doc['scan_id']:05d}_{date.month:02d}_{date.day:02d}_{date.year:04d}.h5"

    combined_h5_filepath = str(Path(export_dir) / Path(filename))
    print(f"{combined_h5_filepath = }")

    def get_dtype(value):
        if isinstance(value, str):
            return h5py.special_dtype(vlen=str)
        if isinstance(value, float):
            return np.float32
        if isinstance(value, int):
            return np.int32
        return type(value)

    with h5py.File(combined_h5_filepath, "x") as h5_file:
        entry_grp = h5_file.require_group("entry")
        data_grp = entry_grp.require_group("data")

        meta_dict = get_detector_parameters_from_tiled(run, det_name)
        for _, v in meta_dict.items():
            meta = v
            break
        current_metadata_grp = h5_file.require_group("entry/instrument/detector")
        for key, value in meta.items():
            if key not in current_metadata_grp:
                dtype = get_dtype(value)
                current_metadata_grp.create_dataset(key, data=value, dtype=dtype)

        # External link
        data_grp["data"] = h5py.ExternalLink(copied_det_file, "entry/data/data")

        # static_motors group
        # static_motors_grp = h5_file.require_group("entry/static_motors")
        # motor_meta_dict = get_motor_metadata(run)
        # for key, value in motor_meta_dict.items():
        #     curr_motor_grp = h5_file.require_group(f"entry/static_motors/{key.split('_')[0]}")
        #     if key not in curr_motor_grp:
        #         dtype = get_dtype(value)
        #         # if need to update key names, do in line below?
        #         curr_motor_grp.create_dataset(key, data=value, dtype=dtype)

        # static_motors group
        static_motors_grp = entry_grp.require_group("static_motors")
        motor_meta_dict = get_motor_metadata(run)
        for motor_name, values in motor_meta_dict.items():
            curr_motor_grp = static_motors_grp.require_group(f"{motor_name}")
            for field, value in values.items():
                if field not in curr_motor_grp:
                    dtype = get_dtype(value)
                    # if need to update key names, do in line below?
                    curr_motor_grp.create_dataset(field, data=value, dtype=dtype)


@flow
def export_edxd_flow(ref):
    print(f"tiled: {tiled.__version__}")
    run = tiled_client_hex[ref]
    copied_det_filename = copy_file(run)
    print(f"{copied_det_filename = }")
    create_combined_file(run, det_name="GeRM", copied_det_file=copied_det_filename)
    print("Done!")
