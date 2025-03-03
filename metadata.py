import re, os, uuid
import shutil
import digital_rf as drf
import pprint
import datetime

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
        self.metadata = {}
        if uuid_str is not None:
            self.metadata["uuid"] = uuid_str
        else:
            self.metadata["uuid"] = uuid.uuid4().hex
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
        comment_lines = []
        try:
            with open(csv_file, "r") as file:
                for line in file:
                    if line.startswith("#"):
                        comment_lines.append(line.strip("# ").strip())
                    else:
                        break
                self.parse_header(comment_lines)
        except FileNotFoundError:
            print(f"Error: File '{csv_file}' not found.")
        except Exception as e:
            print(f"Error: {e}")

    def parse_header(self, lines):
        # Process the first line if it's CSV-style metadata
        csv_parts = lines[0].lstrip("#,").split(",")

        dt = datetime.datetime.fromisoformat(csv_parts[0].replace("Z", "+00:00"))
        formatted_timestamp = dt.strftime("%Y%m%d%H%M%S")
        self.metadata["timestamp"] = formatted_timestamp

        self.metadata.update(
            {
                "station_node_number": csv_parts[1],
                "grid_square": csv_parts[2],
                "lat": float(csv_parts[3]),
                "long": float(csv_parts[4]),
                "elev": float(csv_parts[5]),
                "city_state": csv_parts[6],
                "radio": csv_parts[7],
            }
        )
        lines = lines[2:]  # Remove the first line for further processing

        for line in lines:
            if not line or line.startswith("MetaData"):  # Skip headers
                continue

            # Match key-value pairs with optional comma-separated values
            match = re.match(r"(.+?)\s{2,}(.+)", line)
            if match:
                key, value = match.groups()

                # Convert some values to appropriate data types
                if "," in value and not any(
                    c.isalpha() for c in value
                ):  # Convert to list if numeric and comma-separated
                    value = [v.strip() for v in value.split(",")]
                elif value.replace(".", "", 1).isdigit():  # Convert numeric values
                    value = float(value) if "." in value else int(value)

                # Convert key to snake_case for consistency
                key = key.lower().replace(" ", "_").replace("/", "").strip()
                self.metadata[key] = value

        if "lat,_lon,_elv" in self.metadata:
            del self.metadata["lat,_lon,_elv"]
        if "gps_fix,pdop" in self.metadata:
            # split into two key-pairs
            gps_fix, pdop = self.metadata["gps_fix,pdop"]
            self.metadata["gps_fix"] = int(gps_fix)
            self.metadata["pdop"] = float(pdop)
            del self.metadata["gps_fix,pdop"]
        if "rfdecksn,_logicctrlrsn" in self.metadata:
            rfdecksn, logicctrlrssn = self.metadata["rfdecksn,_logicctrlrsn"]
            self.metadata["rfdecksn"] = rfdecksn
            self.metadata["logicctrlrsn"] = logicctrlrssn
            del self.metadata["rfdecksn,_logicctrlrsn"]

        center_frequencies = [
            float(BEACON_FREQUENCIES[self.metadata[key]])
            for key in self.metadata
            if key.startswith("beacon_") and self.metadata[key] in BEACON_FREQUENCIES
        ]
        self.metadata["center_frequencies"] = center_frequencies

        self.print()

    def write_full(self, index):
        self.write(index, self.metadata)

    def write_secondly(self, index):
        subset = {
            "timestamp": self.metadata["timestamp"],
            "gps_lock": self.metadata["gps_lock"],
            "gps_fix": self.metadata["gps_fix"],
            "sat_count": self.metadata["sat_count"],
            "pdop": self.metadata["pdop"],
            "verify": self.metadata["verify"],
        }
        self.write(index, subset)

    def update_checksum_meta(self, checksum: str):
        """Example checksum: C6e81a9b3V"""
        self.metadata["checksum"] = checksum[1:-1]
        self.metadata["verify"] = checksum[-1]

    def update_timestamp_meta(self, timestamp_str: str):
        """
        # TYYYYMMDDhhmmssLFSP
        T: timestamp indicator
        YYYYMMDDhhmmss: year-month-day-hour-minute-second

        LFSP: L = GPS locked/unlocked (L/U)
            F = Fix (1=no fix, 2=2D fix, 3=3D fix)

            S = satellite count (hex)

            P = Position Dilution of Precision (hex)
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
        pprint.pprint(self.metadata)


if __name__ == "__main__":
    metadata = G2DRFMetadata("drfout/metadatatest", 3600, 60, 8000)
    metadata.extract_header(
        "/home/cuong/drive/GRAPE2-SFTP/grape2/AB1XB/Srawdata/2024-04-08T000000Z_N0001002_RAWDATA.csv"
    )
    with open(
        "/home/cuong/drive/GRAPE2-SFTP/grape2/AB1XB/Srawdata/2024-04-08T000000Z_N0001002_RAWDATA.csv"
    ) as file:
        date = "2024-04-08"
        start_time = int(
            datetime.datetime.strptime(date, "%Y-%m-%d")
            .replace(tzinfo=datetime.timezone.utc)
            .timestamp()
        )
        start_global_index = 0
        first_block = True
        for line in file:
            line = line.strip()
            if line.startswith("T"):
                if first_block:
                    start_global_index = int(
                        start_time * metadata.metadata["ad_sample_rate"]
                    )
                metadata.update_timestamp_meta(line)
            elif line.startswith("C"):
                metadata.update_checksum_meta(line)
                curr_epoch_time = int(
                    datetime.datetime.strptime(
                        metadata.metadata["timestamp"], "%Y%m%d%H%M%S"
                    )
                    .replace(tzinfo=datetime.timezone.utc)
                    .timestamp()
                )
                if first_block:
                    metadata.write_full(curr_epoch_time * metadata.metadata["ad_sample_rate"])
                    first_block = False
                else:
                    metadata.write_secondly(curr_epoch_time * metadata.metadata["ad_sample_rate"])
            else:
                continue
