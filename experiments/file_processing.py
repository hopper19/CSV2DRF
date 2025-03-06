"""
GOAL:   Produce a list/array/matrix that is aware of timestamps, checksums, and data blocks.
        Data blocks are converted from hex to int32.
"""
import glob
import os
import pprint
import numpy as np
import polars as pl

data_only = "../samples/data.txt"
full_file = "../samples/full.txt"

# check for errors
# batch size
# globbing
# QUESTION: what does indexing order mean?

# pdf = (
#     pl.read_csv(
#         full_file,
#         has_header=False,
#         skip_rows=34,
#         infer_schema=False,
#         # three columns of hex strings
#         # schema=pl.Schema({"beacon_{i}": pl.String for i in range(3)}),
#     )
#     .select(pl.all().str.to_integer(base=16, strict=False))  # strict=False if certain strings cannot be converted, they will be replaced with nulls (good for eliminating timestamps and checksums)
#     .drop_nulls()
# )
# print(pdf.head())

##############################################################################
# GOOD STUFFF
def parse_file(search_pattern):
    meta_queries = []
    data_queries = []
    for file in sorted(glob.glob(search_pattern)):
        samples = (
            pl.scan_csv(
                file,
                schema=pl.Schema({"raw": pl.String}),
                skip_rows=33,
                has_header=False,
                separator=chr(0000),
            )
            .select(pl.col("raw").str.split_exact(",", 2))
            .unnest("raw")
        )
        meta_row = samples.filter(pl.any_horizontal(pl.all().is_null())).select(pl.first())
        timestamp = (
            meta_row
            .gather_every(2)
            .with_columns([
                pl.first().str.slice(1, 14).alias("timestamp"),
                pl.first().str.slice(15, 1).alias("gps_lock"),
                pl.first().str.slice(16, 1).cast(pl.UInt8).alias("gps_fix"),
                pl.first().str.slice(17, 1).str.to_integer(base=16).cast(pl.UInt8).alias("sat_count"),
                pl.first().str.slice(18, 1).str.to_integer(base=16).cast(pl.UInt8).alias("pdop")
            ])
            .select(["timestamp", "gps_lock", "gps_fix", "sat_count", "pdop"])
        )
        checksum = ((
            meta_row
            .gather_every(2, offset=1)
            .with_columns([
                pl.first().str.slice(1, 8).alias("checksum"),
                pl.first().str.slice(9, 1).alias("verify"),
            ])
            .select(["checksum", "verify"])
        ))
        data_queries.append(samples.drop_nulls().select(pl.all().str.to_integer(base=16).cast(pl.Int32)))
        meta_queries.append(pl.concat([timestamp, checksum], how="horizontal"))
                            
    return pl.collect_all(data_queries), pl.collect_all(meta_queries)

# meta = extract_metadata(full_file)
# meta_dict = {
#     col: (meta[col].to_numpy().astype(np.uint8) if col in ["gps_fix", "sat_count", "pdop"] else meta[col].to_numpy().astype(str))
#     for col in meta.columns
# }
# meta_dict = {col: meta[col].to_numpy() for col in meta.columns}
# print(meta_dict)
##############################################################################



#######################################################
# SLOOWWWWWWWWWWWW
# timestamps, checksums = [], []
# with open(full_file) as file:
#     for line in file:
#         if line.startswith("T"):
#             timestamps.append(line)
#         elif line.startswith("C"):
#             checksums.append(line)

# print(len(timestamps), len(checksums))
#######################################################
