"""
Utility for writing Grape 2 DigitalRF Data

# TODO: Investigation: How is UUID string used to tie data and metadata together?
# TODO: be more specific with NumPy typehinting

@authors Cuong Nguyen
"""
from typing import List
import digital_rf as drf
import os
import numpy as np
from metadata_writer import G2DRFMetadataWriter as MetaWriter


class G2DRFDataWriter(drf.DigitalRFWriter):
    """
    G2DRFWriter handles the conversion of Grape2 CSV data to Digital RF format.
    """

    def __init__(
        self,
        channel_dir: str | os.PathLike,
        metadata: MetaWriter,
        subdir_cadence_secs: int,
        file_cadence_millisecs: int,
        start_global_index: int,
        fs: int,
        compression_level: int
    ):
        """
        Initialize the G2DRFWriter object.

        Args:
            dataset_dir (str): Directory for DRF dataset storage.
            metadata (Metadata): Metadata dictionary.
            start_global_index (int): Starting global index.
        """
        self.metadata = metadata

        super().__init__(
            channel_dir,
            np.int32,
            subdir_cadence_secs, 
            file_cadence_millisecs,
            start_global_index,
            fs,
            1,
            None,
            compression_level=compression_level,
            checksum=True,
            is_complex=False,
            num_subchannels=3,
            marching_periods=False
        )

        self.hex_to_int = np.vectorize(lambda x: int(x, 16))
        self.hourly_zero_cal = None

    def update_zero_cal(self):
        """
        Update zero calibration values.
        """
        self.hourly_zero_cal = drf.DigitalMetadataReader(self.metadata._metadata_dir).read_latest()[0]["a_d_zero_cal_data"]

    def __process_data_block(self, block: List[str]) -> np.ndarray:
        """
        Process a block of data and convert to a numpy array.
        """
        return (
            self.hex_to_int(np.array(np.char.split(block, sep=",").tolist())).astype(
                np.int32
            )
            # + self.hourly_zero_cal
            + np.array(self.metadata.metadata["ad_zero_cal_data"]).astype(np.int32)
        )
    
    def write_block(self, block: List[str], next_sample: int):
        # if self.hourly_zero_cal is None:
        #     self.update_zero_cal()
        #     raise ValueError(
        #         "Zero calibration data not set. You must call update_zero_cal() "
        #         "at the start of every new file before writing data."
        #     )
        super().rf_write(self.__process_data_block(block), next_sample=next_sample)