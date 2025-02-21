"""
Created on Fri May 24 2024
Last Modified on Saturday February 15 2025

Utility to convert G2 raw data from CSV to DRF

@authors Cuong Nguyen
reference: drf-utils by fventuri
"""

import re, os, sys, uuid, glob, shutil, warnings, datetime
import argparse
import numpy as np
import pandas as pd
import haversine as hs
import digital_rf as drf
from configparser import ConfigParser

# warnings.filterwarnings("ignore", category=DeprecationWarning)


class ConfigLoader:
    def __init__(self, configpath):
        self.config = ConfigParser(interpolation=None)
        self.config.optionxform = str
        self.config.read(configpath)

    def get_global(self):
        return self.config["global"]
    
    def get_subchannel(self, beacon):
        return self.config["subchannels"][beacon]

class CSV2DRFConverter:
    def __init__(self, inputdir, outputdir, config_path=None, uuid_str=None):
        self.inputdir = inputdir
        self.outputdir = outputdir
        self.uuid_str = uuid_str if uuid_str is not None else uuid.uuid4().hex
        if config_path is not None:
            self.config = ConfigLoader(config_path)
        else:
            self.config = ConfigLoader(sys.argv[0].replace(".py", ".conf"))
        self.drf_writer = DRFWriter(self.config)

    def convert(self, date):
        start_time = int(datetime.datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc).timestamp())
        output_dir = os.path.join(self.outputdir, "OBS" + date + "T00-00")
        os.makedirs(output_dir, exist_ok=True)
        search_pattern = os.path.join(self.inputdir, f"{date}*.csv")
        print(f"Searching in: {search_pattern}")

        # Use glob to find all files matching the given date
        data_files = sorted(glob.glob(search_pattern))
        if len(data_files) == 0:
            print(f'No filename found starting with "{date}"')
            return

        # assumption: all files have the same metadata
        metadata = CSVProcessor(data_files[0]).extract_metadata()

        success, channel_dir, start_global_index = self.drf_writer.create_drf_dataset(
            data_files, output_dir, metadata, start_time, self.uuid_str
        )
        if not success:
            raise Exception("Failed to create DRF dataset")
        print(f"Successfully converted data for {date}")

        ok = self.drf_writer.create_drf_metadata(
            channel_dir, metadata, start_global_index, self.uuid_str
        )
        if not ok:
            raise Exception("Failed to create DRF metadata")
        print(f'Successfully created metadata for {date}')


class CSVProcessor:
    def __init__(self, file_path):
        self.file_path = file_path

    def extract_metadata(self):
        """Parse for the metadata from the given CSV file"""
        # Compile a regular expression to match the headers and field values
        pattern = re.compile(r"#\s*(.+?)\s{2,}(.+)")
        metadata = dict()
        with open(self.file_path, "r") as file:
            header = file.readline().strip().split(",")
            metadata["lat"] = float(header[4])
            metadata["long"] = float(header[5])
            metadata["beacons"] = []
            for line in file:
                line = line.strip()
                if not line.startswith("#"):
                    # Read past the comments
                    break
                elif "beacon" in line.lower():
                    metadata["beacons"].append(line.split()[-1])

                # Extract headers and values using findall
                matches = pattern.findall(line)

                # Convert the list of tuples to a dictionary
                metadata.update(
                    {
                        re.sub(r"[^\w]", "_", header).lower(): value
                        for header, value in matches
                    }
                )
        metadata["a_d_sample_rate"] = int(metadata.get("a_d_sample_rate", 8000))
        return metadata

    def find_rows_with_characters(self):
        row_numbers = []
        # NOTE: possible use: track whether this file is complete or there were dropped packets
        t_count = 0
        c_count = 0

        with open(self.file_path, "r") as file:
            for row_number, line in enumerate(file):
                if any(char in line for char in ["#", "T", "C"]):
                    row_numbers.append(row_number)
                if line.startswith("T"):
                    t_count += 1
                elif line.startswith("C"):
                    c_count += 1
        return row_numbers

class DRFWriter:
    def __init__(self, config):
        self.config = config

    def create_drf_metadata(self, channel_dir, metadata, start_global_index, uuid_str):
        subdir_cadence = int(self.config.get_global()["subdir_cadence"])
        file_cadence_secs = int(self.config.get_global()["millseconds_per_file"]) / 1000
        metadatadir = os.path.join(channel_dir, "metadata")
        os.makedirs(metadatadir)
        do = drf.DigitalMetadataWriter(
            metadatadir,
            subdir_cadence,
            file_cadence_secs,  # file_cadence_secs
            metadata["a_d_sample_rate"],  # sample_rate_numerator
            1,  # sample_rate_denominator
            "metadata",  # file_name
        )
        sample = start_global_index
        frequencies = [
            float(self.config.get_subchannel(metadata["beacons"][i])) for i in range(3)
        ]
        data_dict = {
            "uuid_str": uuid_str,
            "lat": np.single(metadata["lat"]),
            "long": np.single(metadata["long"]),
            "center_frequencies": np.ascontiguousarray(frequencies),
        }
        data_dict.update(metadata)
        do.write(sample, data_dict)
        return True

    def create_drf_dataset(self, input_files, dataset_dir, metadata, start_time, uuid_str):
        channel_name = self.config.get_global()["channel_name"]
        subdir_cadence = int(self.config.get_global()["subdir_cadence"])
        millseconds_per_file = int(self.config.get_global()["millseconds_per_file"])
        compression_level = int(self.config.get_global()["compression_level"])
        dtype = np.uint16

        # set up top level directory
        channel_dir = os.path.join(dataset_dir, channel_name)
        shutil.rmtree(dataset_dir, ignore_errors=True)
        os.makedirs(channel_dir)

        print("Writing Digital RF dataset. This will take a while")

        start_global_index = int(start_time * metadata["a_d_sample_rate"])

        with drf.DigitalRFWriter(
            channel_dir,
            dtype,
            subdir_cadence,
            millseconds_per_file,
            start_global_index,
            metadata["a_d_sample_rate"],  # sample_rate_numerator
            1,  # sample_rate_denominator
            uuid_str,
            compression_level,
            False,  # checksum
            False,  # is_complex
            3,  # num_beacons
            True,  # is_continuous TODO: investigate writing gapped blocks in case there is missing data
            False,  # marching_periods
        ) as do:
            for file in input_files:
                with open(file) as fp:
                    zero_adjust = [
                        int(value, 16) - 0x8000
                        for value in metadata["a_d_zero_cal_data"].split(",")
                    ]
                    curr_time_index = 0
                    samples = np.zeros((metadata["a_d_sample_rate"], 3), dtype=dtype)
                    idx = 0
                    print(f"Processing file {file}")
                    csv_metadata = get_metadata(file)
                    if not self.compare_metadata(metadata, csv_metadata):
                        print(
                            "Critical settings have changed! Aborting conversion for this day..."
                        )
                        shutil.rmtree(dataset_dir, ignore_errors=True)
                        return False, None, None
                    for line in fp:
                        line = line.strip()
                        if not line:
                            break
                        elif line.startswith("C"):
                            if line.endswith("V"):
                                do.rf_write(samples, next_sample=curr_time_index)
                            samples = np.zeros(
                                (metadata["a_d_sample_rate"], 3), dtype=dtype
                            )
                            idx = 0
                        elif line.startswith("T"):
                            curr_epoch_time = int(
                                datetime.datetime.strptime(
                                    line[1:15], "%Y%m%d%H%M%S"
                                ).replace(tzinfo=datetime.timezone.utc).timestamp()
                            )
                            curr_time_index = (
                                curr_epoch_time * metadata["a_d_sample_rate"]
                                - start_global_index
                            )
                        elif line.startswith("#"):
                            continue
                        else:
                            samples[idx] = [
                                int(x, 16) + zero_adjust[i]
                                for i, x in enumerate(line.split(","))
                            ]  # the right code
                            idx += 1
        return True, channel_dir, start_global_index

    def compare_metadata(self, prev_metadata, curr_metadata):
        """Determine if critical Grape2 settings have changed. Return True if equivalent"""
        prev_loc = (prev_metadata["lat"], prev_metadata["long"])
        curr_loc = (curr_metadata["lat"], curr_metadata["long"])

        if hs.haversine(prev_loc, curr_loc) * 1000 > 5:
            return False

        critical_keys = [
            "rfgain",
            "antenna",
            "beacon_1_now_decoded",
            "beacon_2_now_decoded",
            "beacon_3_now_decoded",
            "a_d_sample_rate",
        ]

        return all(prev_metadata[key] == curr_metadata[key] for key in critical_keys)


def get_metadata(data_file):
    """Parse for the metadata from the given CSV file"""
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
            if not line.startswith("#"):
                # Read past the comments
                break
            elif "beacon" in line.lower():
                metadata["beacons"].append(line.split()[-1])

            # Extract headers and values using findall
            matches = pattern.findall(line)

            # Convert the list of tuples to a dictionary
            metadata.update(
                {
                    re.sub(r"[^\w]", "_", header).lower(): value
                    for header, value in matches
                }
            )
    metadata["a_d_sample_rate"] = int(metadata.get("a_d_sample_rate", 8000))
    return metadata

if __name__ == "__main__":
    version = "1.1.6"

    parser = argparse.ArgumentParser(description="Grape 2 CSV to DRF Converter")
    parser.add_argument(
        "-i", "--input_dir", help="Input directory containing CSV files", required=True
    )
    parser.add_argument(
        "-o", "--output_dir", help="Output directory for DRF files", required=True
    )
    parser.add_argument("dates", help="date(s) of the data to be converted", nargs="+")
    parser.add_argument("-u", "--uuid", help="User-defined UUID")
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s v{version}",
        help="show program version",
    )

    # Parse the arguments
    args = parser.parse_args()

    converter = CSV2DRFConverter(args.input_dir, args.output_dir, uuid_str=args.uuid)
    for date in args.dates:
        converter.convert(date)
