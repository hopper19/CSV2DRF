"""
Utility to convert G2 raw data from CSV to DRF

# TODO: test source data missing the first three data blocks of the day
# Command: python csv2drf.py -i ~/drive/GRAPE2-SFTP/grape2/AB1XB/Srawdata/ -o drfout/ 2024-04-08

@author Cuong Nguyen
"""

from typing import Union
import shutil
import re, os, sys, glob, datetime
import argparse
import digital_rf as drf
from configparser import ConfigParser
import polars as pl
import numpy as np
import logging
import traceback

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

# Reset any existing logging configuration
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logger = logging.getLogger(__name__)

class CSV2DRFConverter:

    def __init__(
        self,
        input_dir: str,
        date: str,
        output_dir: str,
        compression_level: int = 0,
    ):
        self.input_files = sorted(glob.glob(os.path.join(input_dir, f"{date}*RAWDATA.csv")))
        if not self.input_files:
            raise Exception(f"No files found for {date}")
        self.metadata = {}
        self.sample_rate = 0
        self.__extract_meta_from_header(self.input_files[0])
        # print(self.metadata)
        self.start_global_index = self.__calculate_start_global_index(self.input_files[0])

        obs_dir = os.path.join(output_dir, "OBS" + date + "T00-00")
        shutil.rmtree(obs_dir, ignore_errors=True)

        channel_dir = os.path.join(obs_dir, "ch0")
        os.makedirs(channel_dir, exist_ok=True)

        metadata_dir = os.path.join(channel_dir, "metadata")
        os.makedirs(metadata_dir, exist_ok=True)

        subdir_cadence = 3600
        file_cadence_secs = 60

        self.meta_writer = drf.DigitalMetadataWriter(
            metadata_dir,
            subdir_cadence,
            file_cadence_secs,
            self.sample_rate,
            1,
            "metadata",
        )
        self.data_writer = drf.DigitalRFWriter(
            channel_dir,
            np.int32,
            subdir_cadence,
            file_cadence_secs * 1000,
            self.start_global_index,
            self.sample_rate,
            1,
            None,
            compression_level=compression_level,
            checksum=True,
            is_complex=False,
            num_subchannels=3,
            marching_periods=False,
            is_continuous=False,
        )

    def run(self):
        type_map = {
            "timestamp": "S14",
            "gps_lock": "S1",
            "checksum": "S8",
            "verify": "S1",
        }
        for file in self.input_files:
            try:
                # print(f"Processing {os.path.basename(file)}")
                self.__extract_meta_from_header(file)
                data, meta = self.__parse_file( file)
                samples = (
                    meta["timestamp"]
                    .str.strptime(pl.Datetime, format="%Y%m%d%H%M%S")
                    .dt.epoch(time_unit="s")
                    * self.sample_rate
                )
                self.metadata.update(meta.row(0, named=True))
                meta_dict = {}
                for col in meta.columns:
                    arr = meta[col].to_numpy()[1:]  # first row was be written "manually"
                    if col in type_map:
                        arr = arr.astype(type_map[col])
                    meta_dict[col] = arr
                self.meta_writer.write(samples[0], self.metadata)
                self.meta_writer.write(samples[1:], meta_dict)
                # TEST: performance when using ONLY write blocks
                if len(samples) == 3600:  # no gaps
                    self.data_writer.rf_write(data)
                else:
                    global_sample_arr = samples - self.start_global_index
                    block_sample_arr = np.arange(len(samples)) * self.sample_rate
                    self.data_writer.rf_write_blocks(
                        data, global_sample_arr, block_sample_arr
                    )
            except Exception as e:
                error_message = f"Error processing file {file}"
                logger.error(error_message)
                logger.error(f"Traceback: {traceback.format_exc()}")

    def __parse_file(self, file: str):
        samples = pl.scan_csv(
            file,
            schema=pl.Schema({f"f{i}": pl.String for i in range(3)}),
            comment_prefix="#",
            has_header=False
        )
        uncal_data = samples.drop_nulls().select(pl.all().str.to_integer(base=16).cast(pl.Int32))
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
            uncal_data.with_columns(
                [
                    pl.col(name) + offset
                    for name, offset in zip(
                        uncal_data.collect_schema().names(),
                        self.metadata["ad_zero_cal_data"],
                    )
                ]
            ).collect(),
            pl.concat([timestamp, checksum], how="horizontal").collect(),
        )

    def __calculate_start_global_index(self, filepath):
        with open(filepath) as file:
            line = file.readline()
            while line.startswith("#"):
                line = file.readline()
            return (
                datetime.datetime.strptime(line[1:15], "%Y%m%d%H%M%S")
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
                * self.sample_rate
            )

    def __extract_meta_from_header(self, csv_file: Union[str, os.PathLike]):
        """
        Extract and parse the header from the given CSV file.

        Args:
            csv_file (str): Path to the CSV file.
        """
        comment_lines = []
        with open(csv_file, "r") as file:
            line = file.readline()
            while line.startswith("#"):
                line = line.lstrip("#").strip()
                comment_lines.append(line)    
                line = file.readline()
            self.__extract_metadata(comment_lines)
            self.__cleanup_metadata()
            self.__calculate_center_frequencies()
        self.sample_rate = int(
            self.metadata["ad_sample_rate"]
            if "ad_sample_rate" in self.metadata
            else 8000
        )
        # pprint.pprint(self.metadata)

    def __extract_metadata(self, lines: list[str]):
        """
        Extract metadata from the header lines

        Args:
            line (str): First line of metadata.
        """
        csv_parts = lines[0].split(",")
        self.metadata.update(
            {
                "timestamp": datetime.datetime.fromisoformat(
                    csv_parts[1].replace("Z", "+00:00")
                ).strftime("%Y%m%d%H%M%S"),
                "station_node_number": csv_parts[2],
                "grid_square": csv_parts[3],
                "lat": float(csv_parts[4]),
                "long": float(csv_parts[5]),
                "elev": float(csv_parts[6]),
                "city_state": csv_parts[7],
                "radio": csv_parts[8],
            }
        ) 
        for line in lines[1:]:
            if not line or line.startswith("MetaData"):
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
                (int(x, 16) - 0x8000) for x in self.metadata["ad_zero_cal_data"]
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


def main():
    parser = argparse.ArgumentParser(description="Grape 2 CSV to DRF Converter")
    parser.add_argument(
        "-i", "--input_dir", help="Input directory containing CSV files", required=True
    )
    parser.add_argument(
        "-o", "--output_dir", help="Output directory for DRF files", required=True
    )
    parser.add_argument("dates", help="date(s) of the data to be converted", nargs="+")
    parser.add_argument(
        "-c",
        "--compression",
        type=int,
        default=0,
        help="Compression level (0-9, default: 0 for no compression)",
    )
    parser.add_argument(
        "-l",
        "--log_file",
        help="Path to the log file (if not specified, logging is disabled)",
    )

    args = parser.parse_args()
    if args.log_file:
        logging.basicConfig(
            filename=args.log_file,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        # Log the command that was used
        command = f"python {' '.join(sys.argv)}"
        logger.info(f"Command: {command}")

    for date in args.dates:
        try:
            converter = CSV2DRFConverter(
                args.input_dir, date, args.output_dir, args.compression
            )
            converter.run()
        except Exception as e:
            error_message = f"Error applying conversion for date {date}"
            logger.error(error_message)

if __name__ == "__main__":
    main()
