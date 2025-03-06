import glob
import digital_rf as drf
import shutil
import os
import numpy as np
from file_processing import parse_file
import polars as pl
import datetime

# TODO: extract header
version = "4.0"
date = "2024-04-08"
fs = 8000
start_global_index = int(
            datetime.datetime.strptime(date, "%Y-%m-%d")
            .replace(tzinfo=datetime.timezone.utc)
            .timestamp()
    )*fs

shutil.rmtree("/tmp/ch0", ignore_errors=True)
os.makedirs("/tmp/ch0/metadata", exist_ok=True)
dmw = drf.DigitalMetadataWriter(
    "/tmp/ch0/metadata",
    subdir_cadence_secs=3600,
    file_cadence_secs=60,
    sample_rate_numerator=8000,
    sample_rate_denominator=1,
    file_name="metadata"
)

drw = drf.DigitalRFWriter(
    "/tmp/ch0",
    np.int32,
    subdir_cadence_secs=3600,
    file_cadence_millisecs=60000,
    start_global_index=start_global_index,
    sample_rate_numerator=fs,
    sample_rate_denominator=1,
    is_complex=False,
    num_subchannels=3,
    compression_level=0,
    checksum=True,
    marching_periods=False,
    is_continuous=False,
)

type_map = {
    "timestamp": "S14",
    "gps_lock": "S1",
    "checksum": "S8",
    "verify": "S1",
}
search_pattern = os.path.join("/home/cuong/drive/GRAPE2-SFTP/grape2/AB1XB/Srawdata", "2024-04-08*.csv")
for file in sorted(glob.glob(search_pattern)):
    print(f"Processing {file}")
    data, meta = parse_file(file)

    # add header meta to first block
    meta_dict = {}
    for col in meta.columns:
        arr = meta[col].to_numpy()
        if col in type_map:
            arr = arr.astype(type_map[col])
        meta_dict[col] = arr

    epochs = meta["timestamp"].str.strptime(pl.Datetime, format="%Y%m%d%H%M%S").dt.epoch(time_unit='s')
    samples = epochs * fs
    dmw.write(samples.to_list(), meta_dict)
    if len(epochs) == 3600:  # no gaps
        drw.rf_write(data)
    else:
        global_sample_arr = samples - start_global_index
        block_sample_arr = np.arange(len(epochs)) * fs
        drw.rf_write_blocks(data, global_sample_arr, block_sample_arr)