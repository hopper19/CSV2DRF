from tracemalloc import start
import re, os, uuid
import shutil
import digital_rf as drf
import pprint
import datetime
import logging

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


class G2DRFMetadata(drf.DigitalMetadataWriter):

    def __init__(
        self,
        dir: str,
        subdir_cadence: int,
        file_cadence_secs: int,
        fs: int,
        uuid_str=None,
    ):
        """
        Initialize the G2DRFMetadata object.

        Args:
            dir (str): Directory for metadata storage.
            subdir_cadence (int): Subdirectory cadence.
            file_cadence_secs (int): File cadence in seconds.
            fs (int): Sample rate numerator.
            uuid_str (str, optional): UUID string. Defaults to None.
        """
        self.metadata = {"uuid": uuid_str or uuid.uuid4().hex}
        shutil.rmtree(dir, ignore_errors=True)
        os.makedirs(dir)

        super().__init__(
            dir,
            subdir_cadence,
            file_cadence_secs,  # file_cadence_secs
            fs,  # sample_rate_numerator
            1,  # sample_rate_denominator
            "metadata",  # file_name
        )

    def extract_header(self, csv_file: str):
        """
        Extract and parse the header from the given CSV file.

        Args:
            csv_file (str): Path to the CSV file.
        """
        with open(csv_file, "r") as file:
            comment_lines = [line.strip("# ").strip() for line in file if line.startswith("#")]
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
        self.print()

    def __process_first_metadata(self, line: str):
        """
        Process the first line of metadata to extract initial values.

        Args:
            line (str): First line of metadata.
        """
        csv_parts = line.lstrip("#,").split(",")
        dt = datetime.datetime.fromisoformat(csv_parts[0].replace("Z", "+00:00"))
        self.metadata.update(
            {
                "timestamp": dt.strftime("%Y%m%d%H%M%S"),
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
                if "," in value and not any(c.isalpha() for c in value):
                    value = [v.strip() for v in value.split(",")]
                elif value.replace(".", "", 1).isdigit():
                    value = float(value) if "." in value else int(value)
                key = key.lower().replace(" ", "_").replace("/", "").strip()
                self.metadata[key] = value

    def __cleanup_metadata(self):
        """
        Clean up and organize the metadata dictionary.
        """
        for key in ["lat,_lon,_elv", "gps_fix,pdop", "rfdecksn,_logicctrlrsn"]:
            if key in self.metadata:
                values = self.metadata.pop(key)
                if key == "gps_fix,pdop":
                    self.metadata["gps_fix"], self.metadata["pdop"] = int(values[0]), float(values[1])
                elif key == "rfdecksn,_logicctrlrsn":
                    self.metadata["rfdecksn"], self.metadata["logicctrlrsn"] = values

    def __calculate_center_frequencies(self):
        """
        Calculate center frequencies based on beacon frequencies.
        """
        self.metadata["center_frequencies"] = [
            float(BEACON_FREQUENCIES[self.metadata[key]])
            for key in self.metadata
            if key.startswith("beacon_") and self.metadata[key] in BEACON_FREQUENCIES
        ]

    def write_full(self, index: int):
        """
        Write the full metadata to the specified index.

        Args:
            index (int): Index to write the metadata.
        """
        self.write(index, self.metadata)

    def write_secondly(self, index: int):
        """
        Write a subset of metadata to the specified index.

        Args:
            index (int): Index to write the metadata.
        """
        subset_keys = ["timestamp", "gps_lock", "gps_fix", "sat_count", "pdop", "verify"]
        subset = {key: self.metadata[key] for key in subset_keys}
        self.write(index, subset)

    def update_checksum_meta(self, checksum: str):
        """
        Update the metadata with the given checksum.
        TODO: example checksum string

        Args:
            checksum (str): Checksum string.
        """
        self.metadata["checksum"] = checksum[1:-1]
        self.metadata["verify"] = checksum[-1]

    def update_timestamp_meta(self, timestamp_str: str):
        """
        Update the metadata with the given timestamp string.

        Timestamp format TYYYYMMDDhhmmssLFSP, where:

        T: timestamp indicator

        YYYYMMDDhhmmss: year-month-day-hour-minute-second
        
        LFSP:
            L = GPS locked/unlocked (L/U)
            F = Fix (1=no fix, 2=2D fix, 3=3D fix)
            S = satellite count (hex)
            P = Position Dilution of Precision (hex)

        Args:
            timestamp_str (str): Timestamp string.
        """
        self.metadata.update(
            {
                "timestamp": timestamp_str[1:15],
                "gps_lock": timestamp_str[15],
                "gps_fix": int(timestamp_str[16]),
                "sat_count": int(timestamp_str[17], 16),
                "pdop": int(timestamp_str[18], 16),
            }
        )

    def print(self):
        """
        Print the metadata in a readable format.
        """
        pprint.pprint(self.metadata)


if __name__ == "__main__":
    def timestamp_to_epoch(timestamp: str):
        return int(
            datetime.datetime.strptime(timestamp, "%Y%m%d%H%M%S")
            .replace(tzinfo=datetime.timezone.utc)
            .timestamp()
        )
    filename = "/home/cuong/drive/GRAPE2-SFTP/grape2/AB1XB/Srawdata/2024-04-08T000000Z_N0001002_RAWDATA.csv"
    metadata = G2DRFMetadata("drfout/metadatatest", 3600, 60, 8000)
    metadata.extract_header(filename)
    start_time = timestamp_to_epoch(metadata.metadata["timestamp"])
    with open(filename) as file:
        start_global_index = 0
        first_block = True
        ad_sample_rate = metadata.metadata["ad_sample_rate"]
        for line in file:
            line = line.strip()
            if line.startswith("T"):
                if first_block:
                    start_global_index = int(start_time * ad_sample_rate)
                metadata.update_timestamp_meta(line)
            elif line.startswith("C"):
                metadata.update_checksum_meta(line)
                curr_epoch_time = timestamp_to_epoch(metadata.metadata["timestamp"])
                curr_index = int(curr_epoch_time * ad_sample_rate)
                if first_block:
                    metadata.write_full(curr_index)
                    first_block = False
                else:
                    metadata.write_secondly(curr_index)
