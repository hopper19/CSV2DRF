"""
Utility to convert G2 raw data from CSV to DRF

# TODO: test source data missing the first three data blocks of the day
# Command: 

@author Cuong Nguyen
"""

import pprint
import shutil
import re, os, sys, glob, datetime
import argparse
import digital_rf as drf
from configparser import ConfigParser
from matplotlib.pylab import f
import polars as pl
import numpy as np
from zmq import has

BEACON_FREQUENCIES = {
    "WWV2p5": 2.5,
    "WWV5": 5,
    "WWV10": 10,
    "WWV15": 15,
    "WWV20": 20,
    "WWV25": 25,
    "CHU3": 3.33,
    "CHU7": 7.85,
    "CHU14": 14.67,
}

class ConfigLoader:
    def __init__(self, configpath):
        self.config = ConfigParser(interpolation=None)
        self.config.read(configpath)

    def get_compression_level(self):
        return int(self.config["global"]["compression_level"])


class CSV2DRFConverter:

    def __init__(
        self,
        input_dir: str,
        date: str,
        output_dir: str,
        config_path,
    ):
        self.input_files = sorted(glob.glob(os.path.join(input_dir, f"{date}*.csv")))
        if not self.input_files:
            raise Exception(f"No files found for {date}")
        self.metadata = {}
        self.__extract_header(self.input_files[0])
        print(self.metadata)
        self.start_global_index = self.__get_first_epoch(self.input_files[0]) * self.metadata["ad_sample_rate"]

        self.obs_dir = os.path.join(output_dir, "OBS" + date + "T00-00")
        shutil.rmtree(self.obs_dir, ignore_errors=True)

        channel_dir = os.path.join(self.obs_dir, "ch0")
        os.makedirs(channel_dir, exist_ok=True)

        metadata_dir = os.path.join(channel_dir, "metadata")
        os.makedirs(metadata_dir, exist_ok=True)

        subdir_cadence = 3600
        file_cadence_secs = 60

        self.config = ConfigLoader(config_path)
        self.meta_writer = drf.DigitalMetadataWriter(
            metadata_dir,
            subdir_cadence,
            file_cadence_secs,
            self.metadata["ad_sample_rate"],
            1,
            "metadata",
        )
        self.data_writer = drf.DigitalRFWriter(
            channel_dir,
            np.int32,
            subdir_cadence,
            file_cadence_secs * 1000,
            self.start_global_index,
            self.metadata["ad_sample_rate"],
            1,
            None,
            compression_level=self.config.get_compression_level(),
            checksum=True,
            is_complex=False,
            num_subchannels=3,
            marching_periods=False,
            is_continuous=False,
        )

    def run(self):
        for file in self.input_files:
            print(f"Processing {os.path.basename(file)}")
            data, meta = self.__parse_file(file)
            epochs = meta["timestamp"].str.strptime(pl.Datetime, format="%Y%m%d%H%M%S").dt.epoch(time_unit='s')
            samples = epochs * self.metadata["ad_sample_rate"]

            self.metadata.update(meta.row(0, named=True))
            type_map = {
                "timestamp": "S14",
                "gps_lock": "S1",
                "checksum": "S8",
                "verify": "S1",
            }
            meta_dict = {}
            for col in meta.columns:
                arr = meta[col].to_numpy()[1:]  # first row was be written "manually"
                if col in type_map:
                    arr = arr.astype(type_map[col])
                meta_dict[col] = arr
            self.meta_writer.write(samples[0], self.metadata)
            self.meta_writer.write(samples[1:].to_list(), meta_dict)
            # TEST: performance when using ONLY write blocks
            if len(epochs) == 3600:  # no gaps
                self.data_writer.rf_write(data)
            else:
                global_sample_arr = samples - self.start_global_index
                block_sample_arr = np.arange(len(epochs)) * self.metadata["ad_sample_rate"]
                self.data_writer.rf_write_blocks(data, global_sample_arr, block_sample_arr)

    def __parse_file(self, file: str):
        samples = pl.scan_csv(
            file,
            schema=pl.Schema({f"f{i}": pl.String for i in range(3)}),
            comment_prefix="#",
            has_header=False
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
        return (
            samples.drop_nulls().select(pl.all().str.to_integer(base=16).cast(pl.Int32)).collect(),
            pl.concat([timestamp, checksum], how="horizontal").collect(),
        )

    def __get_first_epoch(self, filepath):
        epoch = 0
        with open(filepath) as file:
            for line in file:
                if line.startswith("T"):
                    epoch = int(
                        datetime.datetime.strptime(line[1:15], "%Y%m%d%H%M%S")
                        .replace(tzinfo=datetime.timezone.utc)
                        .timestamp()
                    )
                    break
        return epoch

    def __extract_header(self, csv_file: str | os.PathLike):
        """
        Extract and parse the header from the given CSV file.

        Args:
            csv_file (str): Path to the CSV file.
        """
        with open(csv_file, "r") as file:
            comment_lines = [
                line.strip().lstrip("#").strip()
                for line in file
                if line.startswith("#")
            ]
            self.__parse_header(comment_lines)

    def __parse_header(self, lines: list[str]):
        """
        Parse the header lines to extract metadata.

        Args:
            lines (list[str]): List of header lines.
        """
        self.__process_first_metadata(lines[0])
        self.__process_key_value_pairs(lines[2:])  # Skip the first two lines
        self.__cleanup_metadata()
        self.__calculate_center_frequencies()
        # pprint.pprint(self.metadata)

    def __process_first_metadata(self, line: str):
        """
        Process the first line of metadata to extract initial values.

        Args:
            line (str): First line of metadata.
        """
        csv_parts = line.lstrip("#,").split(",")
        self.metadata.update(
            {
                "timestamp": datetime.datetime.fromisoformat(
                    csv_parts[0].replace("Z", "+00:00")
                ).strftime("%Y%m%d%H%M%S"),
                "station_node_number": csv_parts[1],
                "grid_square": csv_parts[2],
                "lat": float(csv_parts[3]),
                "long": float(csv_parts[4]),
                "elev": float(csv_parts[5]),
                "city_state": csv_parts[6],
                "radio": csv_parts[7],
            }
        )

    def __process_key_value_pairs(self, lines: list[str]):
        """
        Process key-value pairs from the remaining lines of metadata.

        Args:
            lines (list[str]): List of metadata lines.
        """
        for line in lines:
            if not line or line.startswith("MetaData"):  # Skip headers
                continue
            match = re.match(r"(.+?)\s{2,}(.+)", line)
            if match:
                key, value = match.groups()
                if "," in value:
                    value = value.split(",")
                elif value.replace(".", "", 1).isdigit():
                    value = float(value) if "." in value else int(value)
                key = key.lower().replace(" ", "_").replace("/", "").strip()
                self.metadata[key] = value

    def __cleanup_metadata(self):
        """
        Clean up, ensure appropriate data types, and organize the metadata dictionary.
        """
        for key in ["lat,_lon,_elv", "gps_fix,pdop", "rfdecksn,_logicctrlrsn"]:
            if key in self.metadata:
                values = self.metadata.pop(key)
                if key == "gps_fix,pdop":
                    self.metadata["gps_fix"], self.metadata["pdop"] = int(
                        values[0]
                    ), float(values[1])
                elif key == "rfdecksn,_logicctrlrsn":
                    self.metadata["rfdecksn"], self.metadata["logicctrlrsn"] = [
                        int(x) for x in values
                    ]
        if "ad_zero_cal_data" in self.metadata:
            self.metadata["ad_zero_cal_data"] = [
                int(x, 16) for x in self.metadata["ad_zero_cal_data"]
            ]

    def __calculate_center_frequencies(self):
        """
        Calculate center frequencies based on beacon frequencies.
        """
        self.metadata["beacons"] = [
            self.metadata.pop(key)
            for key in sorted(self.metadata.keys())
            if key.startswith("beacon_")
        ]
        self.metadata["center_frequencies"] = [
            float(BEACON_FREQUENCIES[beacon]) for beacon in self.metadata["beacons"]
        ]

if __name__ == "__main__":
    version = "4.2"

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

    args = parser.parse_args()

    for date in args.dates:
        CSV2DRFConverter(
            args.input_dir, date, args.output_dir, sys.argv[0].replace(".py", ".conf")
        ).run()
