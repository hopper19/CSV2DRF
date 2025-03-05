"""
Utility to convert G2 raw data from CSV to DRF

# TODO: test source data missing the first three data blocks of the day

@author Cuong Nguyen
"""

import shutil
import re, os, sys, glob, datetime
import argparse
import digital_rf as drf
from configparser import ConfigParser
from metadata_writer import G2DRFMetadataWriter as MetaWriter
from data_writer import G2DRFDataWriter as DataWriter


class ConfigLoader:
    def __init__(self, configpath):
        self.config = ConfigParser(interpolation=None)
        self.config.read(configpath)

    def get_compression_level(self):
        return int(self.config["global"]["compression_level"])


class CSV2DRFConverter:

    def __init__(
        self,
        input_dir: str | os.PathLike,
        date: str,
        output_dir: str | os.PathLike,
        config_path,
    ):
        self.date = date
        self.input_dir = input_dir
        self.obs_dir = os.path.join(output_dir, "OBS" + date + "T00-00")
        shutil.rmtree(self.obs_dir, ignore_errors=True)

        channel_dir = os.path.join(self.obs_dir, "ch0")
        os.makedirs(channel_dir, exist_ok=True)

        metadata_dir = os.path.join(channel_dir, "metadata")
        os.makedirs(metadata_dir, exist_ok=True)

        subdir_cadence = 3600
        file_cadence_secs = 60
        fs = 8000
        start_global_index = int(
            datetime.datetime.strptime(date, "%Y-%m-%d")
            .replace(tzinfo=datetime.timezone.utc)
            .timestamp()
        )

        self.config = ConfigLoader(config_path)
        self.meta_writer = MetaWriter(
            metadata_dir, subdir_cadence, file_cadence_secs, fs
        )
        self.data_writer = DataWriter(
            channel_dir,
            self.meta_writer,
            subdir_cadence,
            file_cadence_secs * 1000,
            start_global_index,
            fs,
            self.config.get_compression_level(),
        )

    def run(self):
        data_files = self.get_hourly_files()
        if not data_files:
            print(f"No data found for {self.date}")
            return

        for file in data_files:
            print(f"Processing {file}")
            self.process_file(file)

    def process_file(self, filename: str | os.PathLike):
        self.meta_writer.extract_header(filename)
        with open(filename) as file:
            first_daily_block = True
            ad_sample_rate = self.meta_writer.metadata["ad_sample_rate"]
            lines = file.readlines()
        i = 0
        curr_time_index = 0
        total_lines = len(lines)
        while i < total_lines:
            line = lines[i].strip()
            if not line or line.startswith("#"):
                i += 1
            elif line.startswith("T"):
                self.meta_writer.update_timestamp_meta(line)
                curr_epoch_time = self.meta_writer.timestamp_to_epoch(
                    self.meta_writer.metadata["timestamp"]
                )
                curr_time_index = int(curr_epoch_time * ad_sample_rate)
                i += 1
            elif line.startswith("C"):
                self.meta_writer.update_checksum_meta(line)
                if first_daily_block:
                    self.meta_writer.write_full(curr_time_index)
                    self.data_writer.update_zero_cal()
                    first_daily_block = False
                else:
                    self.meta_writer.write_secondly(curr_time_index)
                i += 1
            else:
                self.data_writer.write_block(lines[i : i + 8000], curr_time_index)
                i += 8000

    def get_hourly_files(self):
        search_pattern = os.path.join(self.input_dir, f"{self.date}*.csv")
        print(f"Searching in: {search_pattern}")
        return sorted(glob.glob(search_pattern))


if __name__ == "__main__":
    version = "2.0.0"

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

    for date in args.dates:
        CSV2DRFConverter(
            args.input_dir, date, args.output_dir, sys.argv[0].replace(".py", ".conf")
        ).run()
