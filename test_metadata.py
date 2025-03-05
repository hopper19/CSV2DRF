import unittest
from metadata_writer import G2DRFMetadataWriter as MetaWriter
import os
import shutil
import digital_rf as drf
import numpy as np

# TODO: add test csv to github


class TestG2DRFMetadata(unittest.TestCase):

    def setUp(self):
        self.test_dir = "test_metadata_dir"
        shutil.rmtree(self.test_dir, ignore_errors=True)
        os.makedirs(self.test_dir, exist_ok=True)
        self.test_csv = "test.csv"
        self.metadata = MetaWriter(self.test_dir, 3600, 60, 8000)
        self.expected_metadata = {
            "ad_sample_rate": 8000,
            "ad_zero_cal_data": [0x7EDE, 0x7F07, 0x7F2A],
            "antenna": "MLA-30+ active wideband loop",
            "beacons": ["WWV5", "WWV10", "WWV15"],
            "callsign": "AB1XB",
            "center_frequencies": [5.0, 10.0, 15.0],
            "city_state": "Harvard MA",
            "data_controller_version": "0.7.16",
            "elev": 128.6,
            "frequency_standard": "LB GPSDO",
            "gps_acquisition_on": "2024-04-06 21:23:43.740476",
            "gps_fix": 3,
            "grid_square": "FN42el",
            "lat": 42.49834,
            "logicctrlrsn": 1002,
            "long": -71.590725,
            "magdata_version": "0.0.4",
            "pdop": 1.9,
            "picorun_version": "0.5.17",
            "pswssetup_version": 2.27,
            "radio": "Grape 2",
            "radioid1": "G2R1",
            "radioid2": "G2R2",
            "radioid3": "G2R3",
            "rfdecksn": 102,
            "rfgain": 10,
            "station_node_number": "N0001002",
            "system_info": ["RasPi4B/8GB", " RasPi OS Bullseye 6.1.21"],
            "timestamp": "20240408000000",
        }
        with open(self.test_csv, "w") as file:
            file.write(
                "#,2024-04-08T00:00:00Z,N0001002,FN42el,42.498340,-71.590725,128.6,Harvard MA,Grape 2\n"
                "######################################################\n"
                "# MetaData for Grape Gen 2 Station\n"
                "#\n"
                "# Station Node Number      N0001002\n"
                "# Callsign                 AB1XB\n"
                "# Grid Square              FN42el\n"
                "# Lat, Lon, Elv            42.498340,-71.590725,128.6\n"
                "# GPS Fix,PDOP             3,1.9\n"
                "# GPS Acquisition on       2024-04-06 21:23:43.740476\n"
                "# City State               Harvard MA\n"
                "# Radio                    Grape 2\n"
                "# RFGain                   10\n"
                "# RadioID1                 G2R1\n"
                "# RadioID2                 G2R2\n"
                "# RadioID3                 G2R3\n"
                "# Antenna                  MLA-30+ active wideband loop\n"
                "# Frequency Standard       LB GPSDO\n"
                "# System Info              RasPi4B/8GB, RasPi OS Bullseye 6.1.21\n"
                "# RFDeckSN, LogicCtrlrSN   102,1002\n"
                "# Data Controller Version  0.7.16\n"
                "# Picorun Version          0.5.17\n"
                "# magdata Version          0.0.4\n"
                "# PSWSsetup Version        2.27\n"
                "#\n"
                "# Beacon 1 Now Decoded     WWV5\n"
                "# Beacon 2 Now Decoded     WWV10\n"
                "# Beacon 3 Now Decoded     WWV15\n"
                "#\n"
                "# A/D Sample Rate          8000\n"
                "# A/D Zero Cal Data        7ede,7f07,7f2a\n"
                "######################################################\n"
            )

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)
        if os.path.exists(self.test_csv):
            os.remove(self.test_csv)

    def test_extract_header(self):
        self.metadata.extract_header(self.test_csv)
        for key, value in self.expected_metadata.items():
            self.assertEqual(self.metadata.metadata[key], value)

    def test_update_checksum_meta(self):
        self.metadata.update_checksum_meta("C87ddb701V")
        self.assertEqual(self.metadata.metadata["checksum"], "87ddb701")
        self.assertEqual(self.metadata.metadata["verify"], "V")

    def test_update_timestamp_meta(self):
        self.metadata.update_timestamp_meta("T20240408000000L3A1")
        self.assertEqual(self.metadata.metadata["timestamp"], "20240408000000")
        self.assertEqual(self.metadata.metadata["gps_lock"], "L")
        self.assertEqual(self.metadata.metadata["gps_fix"], 0x0003)
        self.assertEqual(self.metadata.metadata["sat_count"], 0x000A)
        self.assertEqual(self.metadata.metadata["pdop"], 0x0001)

    def test_write_full(self):
        self.metadata.extract_header(self.test_csv)
        self.metadata.update_checksum_meta("C87ddb701V")
        self.metadata.update_timestamp_meta("T20240408000000L3A1")

        epoch = self.metadata.timestamp_to_epoch("20240408000000")
        self.metadata.write_full(epoch)

        reader = drf.DigitalMetadataReader(self.test_dir)
        bounds = reader.get_bounds()
        print(reader.read_latest())
        read_metadata = reader.read_latest()[bounds[0]]

        for key, expected_value in self.metadata.metadata.items():
            # Skip UUID as it's generated randomly
            if key == "uuid":
                continue
                
            self.assertIn(key, read_metadata, f"Key {key} missing in read metadata")
            
            actual_value = read_metadata[key]
            
            # Handle numpy arrays
            if isinstance(actual_value, np.ndarray):
                if actual_value.dtype.kind == 'O':
                    # Handle string arrays - convert bytes to strings if needed
                    decoded_actual = [item.decode() for item in actual_value]
                    self.assertEqual(decoded_actual, expected_value, f"Mismatch for key {key}")
                else:
                    # Numeric arrays
                    self.assertTrue(np.array_equal(actual_value, expected_value), f"Mismatch for key {key}")
            # Handle float comparison with appropriate tolerance
            elif isinstance(expected_value, float) and isinstance(actual_value, (int, float)):
                self.assertAlmostEqual(float(actual_value), expected_value, places=5, msg=f"Mismatch for key {key}")
            else:
                # Regular comparison for other types
                self.assertEqual(actual_value, expected_value, f"Mismatch for key {key}")

    def test_write_secondly(self):
        self.metadata.update_checksum_meta("C87ddb701V")
        self.metadata.update_timestamp_meta("T20240408000000L3A1")

        epoch = self.metadata.timestamp_to_epoch("20240408000000")
        self.metadata.write_secondly(epoch)

        expected_secondly_metadata = {
            "timestamp": "20240408000000",
            "gps_lock": "L",
            "gps_fix": 0x0003,
            "sat_count": 0x000A,
            "pdop": 0x0001,
            "verify": "V",
            "checksum": "87ddb701",
        }

        reader = drf.DigitalMetadataReader(self.test_dir)
        bounds = reader.get_bounds()
        read_metadata = reader.read_latest()[bounds[0]]

        for key, value in expected_secondly_metadata.items():
            self.assertEqual(read_metadata[key], value)

    def test_timestamp_to_epoch(self):
        self.assertEqual(self.metadata.timestamp_to_epoch("20240408000000"), 1712534400)


if __name__ == "__main__":
    unittest.main()
