import logging

import pandas as pd
from zenml import step


class IngestData:
    """
    Data ingestion class which ingests data from the source and returns a DataFrame.
    """

    def __init__(self, data_path: str):
        """Initialize the data ingestion class."""
        self.data_path = data_path

    def get_data(self) -> pd.DataFrame:
        """
        args:
        data_path: path to the data
        """

        logging.info(f"ingesting data from {self.data_path}")
        return pd.read_csv(self.data_path)


@step
def ingest_df(data_path: str) -> pd.DataFrame:
    """

        ingesting data from the data_path
    Args:
        data_path : path to the data
    Returns:
        df: pd.DataFrame the ingested data
    """
    try:
        ingest_data = IngestData(data_path)
        df = ingest_data.get_data()
        return df
    except Exception as e:
        logging.error(f"error while ingesting data: {e}")


