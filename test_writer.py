"""
@author Cuong Nguyen
"""

import shutil
import os
import unittest
import numpy as np
import digital_rf as drf
from data_writer import G2DRFDataWriter as DataWriter
from metadata_writer import G2DRFMetadataWriter as MetaWriter


class TestG2DRFWriter(unittest.TestCase):
    def setUp(self):
        self.dataset_dir = "test_dataset_dir"
        shutil.rmtree(self.dataset_dir, ignore_errors=True)
        os.makedirs(self.dataset_dir)
        self.channel_dir = os.path.join(self.dataset_dir, "ch0")

        metadata_dir = os.path.join(self.channel_dir, "metadata")
        os.makedirs(metadata_dir)

        # start time 2024-04-08 00:00:00
        epoch = 1712534400
        subdir_cadence = 3600
        file_cadence_sec = 60
        self.fs = 8000

        self.metadata = MetaWriter(
            metadata_dir,
            subdir_cadence,
            file_cadence_sec,
            self.fs,
        )

        self.writer = DataWriter(
            self.channel_dir,
            self.metadata,
            subdir_cadence,
            file_cadence_sec * 1000,
            epoch * self.fs,
            self.fs,
            0,
        )

        self.test_data_block = self.generate_data_block()
        print(self.test_data_block)

    def tearDown(self):
        shutil.rmtree(self.dataset_dir, ignore_errors=True)

    def generate_data_block(self):
        """
        Randomly generate a numpy array with each row containing
        a string sequence of three 4-digit 16-bit hex integers separated by commas.
        """
        # Generate a (num_rows x 3) array of random 16-bit integers (0 to 65535)
        random_ints = np.random.randint(0,
                                        65536,
                                        size=(self.fs, 3),
                                        dtype=np.uint16)

        # Convert each integer to a 4-digit hex string and join them with commas for each row
        hex_rows = []
        for row in random_ints:
            # Format each number as a 4-digit hexadecimal string (uppercase)
            hex_strs = [f"{num:04X}" for num in row]
            # Join the three hex strings with commas
            hex_rows.append(",".join(hex_strs))

        # Convert the list of string rows into a numpy array and return
        return np.array(hex_rows)

    def test_process_data_block(self):
        """
        Test the process_data_block method.
        """
        # processed_block = self.writer.process_data_block(self.test_data_block)
        # print(processed_block)



if __name__ == "__main__":
    unittest.main()
