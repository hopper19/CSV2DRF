"""
Created on Fri May 24 2024

Utility to convert G2 raw data from CSV to DRF

@authors Cuong Nguyen
reference: drf-utils by fventuri

Note: Remember to change version number in main
Date        Version     Author      Comments

"""
import re, os, sys, uuid, glob, shutil, warnings, datetime
import argparse
import numpy as np
import pandas as pd
import haversine as hs
import digital_rf as drf
from configparser import ConfigParser

warnings.filterwarnings("ignore", category=DeprecationWarning)

class DatasetCreationError(Exception):
    """Exception raised for errors in the dataset creation."""
    def __init__(self, message="Failed to create DRF dataset"):
        self.message = message
        super().__init__(self.message)

class MetadataCreationError(Exception):
    """Exception raised for errors in the metadata creation."""
    def __init__(self, message="Failed to create DRF metadata"):
        self.message = message
        super().__init__(self.message)

def find_rows_with_characters(file_path):
    row_numbers = []
    with open(file_path, "r") as file:
        for row_number, line in enumerate(file):
            if any(char in line for char in ["#", "T", "C"]):
                row_numbers.append(row_number)
    return row_numbers

def process_file(file_path):
    return  pd.read_csv(
                file_path,
                names=range(3),
                skiprows=find_rows_with_characters(file_path),
                header=None,
                converters={col: (lambda x: np.array(int(x, 16)).astype(np.int16)) for col in range(3)},
            ).to_numpy()

def get_metadata(data_file):
    ''' Parse for the metadata from the given CSV file '''
    # Compile a regular expression to match the headers and field values
    pattern = re.compile(r"#\s*(.+?)\s{2,}(.+)")
    metadata = dict()    
    with open(data_file, "r") as file:
        header = file.readline().strip().split(",")
        metadata["lat"] = float(header[4])
        metadata["long"] = float(header[5])
        metadata["beacons"] = []
        for line in file:
            line = line.strip()
            if not line.startswith('#'):
                # Read past the comments
                break
            elif "beacon" in line.lower():
                metadata["beacons"].append(line.split()[-1])

            # Extract headers and values using findall
            matches = pattern.findall(line)
            
            # Convert the list of tuples to a dictionary
            metadata.update({
                re.sub(r'[^\w]', '_', header).lower(): value
                for header, value in matches
            })
    metadata["a_d_sample_rate"] = int(metadata["a_d_sample_rate"])
    return metadata

        
def compare_metadata(prev_metadata, curr_metadata):
    ''' Determine if critical Grape2 settings have changed. Return True if equivalent '''
    prev_loc = (prev_metadata["lat"], prev_metadata["long"])
    curr_loc = (curr_metadata["lat"], curr_metadata["long"])

    if hs.haversine(prev_loc, curr_loc) * 1000 > 5:
        return False

    critical_keys = ["rfgain", "antenna", "beacon_1_now_decoded", "beacon_2_now_decoded", "beacon_3_now_decoded", "a_d_sample_rate"]

    return all(prev_metadata[key] == curr_metadata[key] for key in critical_keys)


def create_drf_dataset(input_files, dataset_dir, config_global, metadata, start_time, uuid_str):
    channel_name = config_global['channel_name']
    subdir_cadence = int(config_global['subdir_cadence'])
    millseconds_per_file = int(config_global['millseconds_per_file'])
    compression_level = int(config_global['compression_level'])
    dtype = np.uint16
    
    # set up top level directory
    channel_dir = os.path.join(dataset_dir, channel_name)
    shutil.rmtree(dataset_dir, ignore_errors=True)
    os.makedirs(channel_dir)

    print('writing Digital RF dataset. This will take a while', file=sys.stderr)
            
    start_global_index = int(start_time * metadata["a_d_sample_rate"])

    with drf.DigitalRFWriter(channel_dir,
                             dtype,
                             subdir_cadence,
                             millseconds_per_file,
                             start_global_index,
                             metadata["a_d_sample_rate"],   # sample_rate_numerator
                             1,                             # sample_rate_denominator
                             uuid_str,
                             compression_level,
                             False,                         # checksum
                             False,                         # is_complex
                             3,                             # num_beacons
                             True,                          # is_continuous
                             False                          # marching_periods
                            ) as do:
        for data_file in input_files:
            with open(data_file) as fp:
                zero_adjust = [int(value, 16) for value in metadata["a_d_zero_cal_data"].split(',') - 0x8000] 
                curr_time_index = 0
                samples = np.zeros((metadata["a_d_sample_rate"], 3), dtype=dtype)
                idx = 0
                print(f"Processing file {data_file}")
                csv_metadata = get_metadata(data_file)
                if not compare_metadata(metadata, csv_metadata):
                    print("Critical settings have changed! Aborting conversion for this day...")
                    shutil.rmtree(dataset_dir, ignore_errors=True)
                    return False, None, None
                for line in fp:
                    line = line.strip()
                    if not line:
                        break
                    elif line.startswith('C'):
                        if line.endswith('V'):
                            do.rf_write(samples, next_sample=curr_time_index)
                        samples = np.zeros((metadata["a_d_sample_rate"], 3), dtype=dtype)
                        idx = 0
                    elif line.startswith('T'):
                        curr_epoch_time = int(datetime.datetime.strptime(line[1:15], "%Y%m%d%H%M%S").strftime("%s"))
                        curr_time_index = curr_epoch_time * metadata["a_d_sample_rate"] - start_global_index 
                    elif line.startswith('#'): 
                        continue
                    else:
                        samples[idx] = [int(x, 16) + zero_adjust[i] for i, x in enumerate(line.split(','))] # the right code
                        idx += 1

    return True, channel_dir, start_global_index

def create_drf_metadata(channel_dir, config, metadata, start_global_index, uuid_str):
    subdir_cadence = int(config['global']['subdir_cadence'])
    file_cadence_secs = int(config['global']['millseconds_per_file'])/1000
    metadatadir = os.path.join(channel_dir, 'metadata')
    os.makedirs(metadatadir)
    do = drf.DigitalMetadataWriter(metadatadir,
                                   subdir_cadence,
                                   file_cadence_secs,  # file_cadence_secs
                                   metadata["a_d_sample_rate"],      # sample_rate_numerator 
                                   1,                # sample_rate_denominator
                                   'metadata'        # file_name
                                  )
    sample = start_global_index
    frequencies = [float(config['subchannels'][metadata["beacons"][i]]) for i in range(3)]
    data_dict = {
        'uuid_str': uuid_str,
        'lat': np.single(metadata["lat"]),
        'long': np.single(metadata["long"]),
        'center_frequencies': np.ascontiguousarray(frequencies)
    }
    data_dict.update(metadata)
    do.write(sample, data_dict)
    return True

def main():
    version = "1.00"
    
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Grape 2 CSV to DRF Converter")
        
    # Add the argument
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s v{version}",
        help="show program version",
    )
    parser.add_argument(
        "dates", help="date(s) of the data to be converted", type=str, nargs="*", default=sys.stdin
    )
    parser.add_argument("-u", "--uuid", help="User-defined UUID", default=None)

    # Parse the arguments
    args = parser.parse_args()
    
    g2_dir = '/home/cuong/drive/GRAPE2-SFTP/grape2/AB1XB'
    input_dir = os.path.join(g2_dir, 'Srawdata')
    uuid_str = args.uuid if args.uuid is not None else uuid.uuid4().hex
    
    configfile = sys.argv[0].replace('.py', '.conf')
    config = ConfigParser(interpolation=None)
    config.optionxform = str
    config.read(configfile)
        
    for date in args.dates:
        ymd = [int(x) for x in date.split('-')]
        # NOTE: possible error: what if the first written timestamp is NOT at the first second of that day
        start_time = int(datetime.datetime.strptime(date, "%Y-%m-%d").strftime('%s'))
        output_dir = os.path.join(g2_dir, 'Sdrf/OBS' + date + 'T00-00')
        os.makedirs(output_dir, exist_ok=True)
        print(f"--- CONVERTING DATA INTO DRF FOR {date} ---")
        search_pattern = os.path.join(input_dir, f"{date}*.csv")
        print(f"Searching in: {search_pattern}")

        
        # Use glob to find all files matching the given date
        data_files = sorted(glob.glob(search_pattern))
        if len(data_files) == 0:
            print(f"No filename found starting with \"{date}\"")
            continue

        metadata = get_metadata(data_files[0])
        
        ok, channel_dir, start_global_index = create_drf_dataset(data_files, output_dir, config['global'], metadata, start_time, uuid_str)
        print('create_drf_dataset returned', ok, file=sys.stderr)
        if not ok:
            raise DatasetCreationError()
        
        ok = create_drf_metadata(channel_dir, config, metadata, start_global_index, uuid_str)
        print('create_drf_metadata returned', ok, file=sys.stderr)
        if not ok:
            raise MetadataCreationError()
        print()
        
    print("Exiting python combined processing program gracefully")
    
if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(str(e))
