import unittest
from metadata import G2DRFMetadata as Metadata
import os
import shutil
import digital_rf as drf

class TestG2DRFMetadata(unittest.TestCase):

    def setUp(self):
        self.test_dir = "test_metadata_dir"
        self.test_csv = "test.csv"
        self.metadata = Metadata(self.test_dir, 3600, 60, 8000)
        with open(self.test_csv, "w") as file:
            file.write("""#,2024-04-08T00:00:00Z,N0001002,FN42el,42.498340,-71.590725,128.6,Harvard MA,Grape 2
            ######################################################
            # MetaData for Grape Gen 2 Station
            #
            # Station Node Number      N0001002
            # Callsign                 AB1XB
            # Grid Square              FN42el
            # Lat, Lon, Elv            42.498340,-71.590725,128.6
            # GPS Fix,PDOP             3,1.9
            # GPS Acquisition on       2024-04-06 21:23:43.740476
            # City State               Harvard MA
            # RFGain                   10
            # Antenna                  MLA-30+ active wideband loop
            # Frequency Standard       LB GPSDO
            # System Info              RasPi4B/8GB, RasPi OS Bullseye 6.1.21
            # RFDeckSN, LogicCtrlrSN   102,1002
            # Data Controller Version  0.7.16
            #
            # Beacon 1 Now Decoded     WWV5
            # Beacon 2 Now Decoded     WWV10
            # Beacon 3 Now Decoded     WWV15
            #
            # A/D Sample Rate          8000
            # A/D Zero Cal Data        7ede,7f07,7f2a
            #
            ######################################################""")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)
        if os.path.exists(self.test_csv):
            os.remove(self.test_csv)

    def test_initialization(self):
        self.assertTrue(os.path.exists(self.test_dir))
        self.assertIn("uuid", self.metadata.metadata)

    def test_extract_header(self):
        expected_metadata = {"
            "ad_sample_rate": 8000,
            "ad_zero_cal_data": [0x7ede, 0x7f07, 0x7f2a],
            "station_node_number": "N0001002",
            "callsign": "AB1XB",
            "grid_square": "FN42el",
            "lat": 42.498340,
            "long": -71.590725,
            "elev": 128.6,
            "city_state": "Harvard MA",
            "radio": "Grape 2"}
        self.metadata.extract_header(self.test_csv)
        self.assertIn("timestamp", self.metadata.metadata)
        self.assertIn("station_node_number", self.metadata.metadata)

    def test_update_checksum_meta(self):
        self.metadata.update_checksum_meta("C1234567890")
        self.assertEqual(self.metadata.metadata["checksum"], "123456789")
        self.assertEqual(self.metadata.metadata["verify"], "0")

    def test_update_timestamp_meta(self):
        self.metadata.update_timestamp_meta("T20240408000000L3A1")
        self.assertEqual(self.metadata.metadata["timestamp"], "20240408000000")
        self.assertEqual(self.metadata.metadata["gps_lock"], "L")
        self.assertEqual(self.metadata.metadata["gps_fix"], 3)
        self.assertEqual(self.metadata.metadata["sat_count"], 10)
        self.assertEqual(self.metadata.metadata["pdop"], 1)

    def test_write_full(self):
        # Setup test data
        self.metadata.metadata.update({
            "timestamp": "20240408000000",
            "gps_lock": "L",
            "gps_fix": 3,
            "sat_count": 10,
            "pdop": 1,
            "verify": "V",
            "test_field": "test_value"
        })
        
        # Write metadata at specific index
        test_index = 800000
        self.metadata.write_full(test_index)
        
        # Read back and verify
        reader = drf.DigitalMetadataReader(self.test_dir)
        read_metadata = reader.read_latest()[test_index]

        # Check that all fields were written
        self.assertEqual(read_metadata["timestamp"], "20240408000000")
        self.assertEqual(read_metadata["gps_lock"], "L")
        self.assertEqual(read_metadata["test_field"], "test_value")
        self.assertEqual(read_metadata["uuid"], self.metadata.metadata["uuid"])

    def test_write_secondly(self):
        # Setup test data
        self.metadata.metadata.update({
            "timestamp": "20240408000000",
            "gps_lock": "L",
            "gps_fix": 3,
            "sat_count": 10,
            "pdop": 1,
            "verify": "V",
            "test_field": "test_value"
        })
        
        # Write metadata at specific index
        test_index = 800000
        self.metadata.write_secondly(test_index)
        
        # Read back and verify
        reader = drf.DigitalMetadataReader(self.test_dir)
        read_metadata = reader.read_latest()[test_index]
        
        # Check that only subset fields were written
        self.assertEqual(read_metadata["timestamp"], "20240408000000")
        self.assertEqual(read_metadata["gps_lock"], "L")
        self.assertEqual(read_metadata["sat_count"], 10)
        self.assertNotIn("test_field", read_metadata)

if __name__ == "__main__":
    unittest.main()

