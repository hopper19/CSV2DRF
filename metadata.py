"""
Created February 23, 2025

Object to store and extract metadata of Grape 2 DigitalRF metadata

TODO: ask Bill and John for data types of each metadata field

@author Cuong Nguyen
"""
import re
import pprint

class G2Metadata:
    @staticmethod
    def extract_metadata(csv_file: str):
        """
        Extracts metadata from a CSV file containing Grape 2 DigitalRF metadata.

        Args:
            csv_file (str): The path to the CSV file.

        Returns:
            dict: A dictionary containing the extracted metadata.
        """
        comment_lines = []
        try:
            with open(csv_file, "r") as file:
                for line in file:
                    if line.startswith('#'):
                        comment_lines.append(line.strip())
                    else:
                        break
                return G2Metadata.parse_comments(comment_lines)
        except FileNotFoundError:
            print(f"Error: File '{csv_file}' not found.")
        except Exception as e:
            print(f"Error: {e}")

    @staticmethod
    def parse_comments(lines):
        metadata = {}

        # Process the first line if it's CSV-style metadata
        csv_parts = lines[0].lstrip('#,').split(',')
        metadata.update({
            "timestamp": csv_parts[0],
            "station_node_number": csv_parts[1],
            "grid_square": csv_parts[2],
            "lat": float(csv_parts[3]),
            "long": float(csv_parts[4]),
            "elev": float(csv_parts[5]),
            "city_state": csv_parts[6],
            "radio": csv_parts[7],
        })
        lines = lines[1:]  # Remove the first line for further processing

        for line in lines:
            line = line.strip("# ")  # Remove leading # and spaces
            if not line or line.startswith("MetaData"):  # Skip headers
                continue

            # Match key-value pairs with optional comma-separated values
            match = re.match(r"(.+?)\s{2,}(.+)", line)
            if match:
                key, value = match.groups()

                # Convert some values to appropriate data types
                if ',' in value and not any(
                        c.isalpha() for c in
                        value):  # Convert to list if numeric and comma-separated
                    value = [v.strip() for v in value.split(',')]
                elif value.replace('.', '', 1).isdigit():  # Convert numeric values
                    value = float(value) if '.' in value else int(value)

                # Convert key to snake_case for consistency
                key = key.lower().replace(" ", "_").replace("/", "").strip()
                metadata[key] = value

        if "lat,_lon,_elv" in metadata:
            del metadata["lat,_lon,_elv"]
        if "gps_fix,pdop" in metadata:
            # split into two key-pairs
            gps_fix, pdop = metadata["gps_fix,pdop"]
            metadata["gps_fix"] = gps_fix
            metadata["pdop"] = float(pdop)
            del metadata["gps_fix,pdop"]
        if "rfdecksn,_logicctrlrsn" in metadata:
            rfdecksn, logicctrlrssn = metadata["rfdecksn,_logicctrlrsn"]
            metadata["rfdecksn"] = rfdecksn
            metadata["logicctrlrsn"] = logicctrlrssn
            del metadata["rfdecksn,_logicctrlrsn"]

        # pprint.pprint(metadata)  # Print the metadata dictionary for debugging

        return metadata

# pprint.pprint(
#     G2Metadata.extract_metadata(
#         "/home/cuong/drive/GRAPE2-SFTP/grape2/AB1XB/Srawdata/2024-04-08T000000Z_N0001002_RAWDATA.csv"
#     )
# )
