import numpy as np
import sys

zero_cal = np.array([0x7EDE, 0x7F07, 0x7F2A]) - 0x8000

hex_to_int = np.vectorize(lambda x: int(x, 16))


def process_data_block(block):
    return (
        hex_to_int(np.array(np.char.split(block, sep=",").tolist())).astype(np.int32)
        + zero_cal
    )

data_only = "../samples/data.txt"
full_file = "../samples/full.txt"

###########################################################################
# with open(full_file) as f:
#     lines = f.readlines()

# print("Total processed blocks:")
# count = 0
      
# i = 0 
# total = len(lines)
# while i < total:
#     line = lines[i].strip()

#     if not line or line.startswith("#") or line.startswith("T") or line.startswith("C"):
#         i += 1
#         continue
#     else:
#         process_data_block(lines[i : i + 8000])
#         i += 8000
#         count += 1
# #         print(f"{count}", end="\r", flush=True)

###############################################################
# 1 block ==> 1 second
# 3600 blocks ==> 1 hour
# 86400 blocks ==> 1 day

with open(data_only) as f:
    lines = f.readlines()
print("Processing", sys.argv[1], "blocks")
for _ in range(int(sys.argv[1])):
    process_data_block(lines)
    # print(f"{_}", end="\r", flush=True)

##################################################
# Complete processing including finding the start of each block
# 2m.48s