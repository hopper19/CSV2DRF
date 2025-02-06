# ----------------------------------------------------------------------------
# Copyright (c) 2017 Massachusetts Institute of Technology (MIT)
# All rights reserved.
#
# Distributed under the terms of the BSD 3-clause license.
#
# The full license is in the LICENSE file, distributed with this software.
# ----------------------------------------------------------------------------
"""A simple example of writing Digital Metadata with python.

Now writes data into two levels of dictionaries/groups. API allow any finite
number of levels.

"""
import os
import shutil
import time

import digital_rf
import numpy as np

data_dir = "hdf5/ch0"
metadata_dir = os.path.join(data_dir, "metadata")
subdirectory_cadence_seconds = 3600
file_cadence_seconds = 1
samples_per_second_numerator = 8000
samples_per_second_denominator = 1
file_name = "metadata"
stime = int(time.time())

shutil.rmtree(metadata_dir, ignore_errors=True)
os.makedirs(metadata_dir)

dmw = digital_rf.DigitalMetadataWriter(
    metadata_dir,
    subdirectory_cadence_seconds,
    file_cadence_seconds,
    samples_per_second_numerator,
    samples_per_second_denominator,
    file_name,
)
print("file created okay")

data_dict = {}
start_idx = int(np.uint64(stime * dmw.get_samples_per_second()))
data_dict["callsign"] = "KC3UAX"


dmw.write(start_idx, data_dict)
print("metadat written okay")
