import glob
import digital_rf as drf
import shutil
import os
import numpy as np
from file_processing import parse_file
import polars as pl
import datetime

# TODO: extract header
version = "2.0"
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
    marching_periods=False
)

search_pattern = os.path.join("/home/cuong/drive/GRAPE2-SFTP/grape2/AB1XB/Srawdata", "2024-04-08*.csv")
for file in sorted(glob.glob(search_pattern)):
    print(f"Processing {file}")
    data, meta = parse_file(file)
    data = data.to_numpy().astype(np.int32)
    meta_dict = {col: meta[col].to_numpy() for col in meta.columns}
    meta_dict["timestamp"] = meta_dict["timestamp"].astype("S14")
    meta_dict["gps_lock"] = meta_dict["gps_lock"].astype("S1")
    meta_dict["gps_fix"] = meta_dict["gps_fix"].astype("uint8")
    meta_dict["sat_count"] = meta_dict["sat_count"].astype("uint8")
    meta_dict["pdop"] = meta_dict["pdop"].astype("uint8")
    meta_dict["checksum"] = meta_dict["checksum"].astype("S8")
    meta_dict["verify"] = meta_dict["verify"].astype("S1")
    epochs = meta["timestamp"].str.strptime(pl.Datetime, format="%Y%m%d%H%M%S").dt.epoch(time_unit='s')
    samples = epochs * fs
    dmw.write(samples.to_list(), meta_dict)
    if len(samples) == 3600:  # no gaps
        drw.rf_write(data)
    else:
        global_sample_arr = samples - start_global_index
        block_sample_arr = np.arange(len(samples)) * fs
        drw.rf_write_blocks(data, global_sample_arr, block_sample_arr)