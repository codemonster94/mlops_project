import logging
from abc import ABC, abstractmethod
import numpy as np
import pandas as pd
from typing import Union
from sklearn.model_selection import train_test_split


class DataStrategy(ABC):
    """
    abstract class defining strategy for handling data
    """

    @abstractmethod
    def handle_data(self, data: pd.DatFrame) -> Union[pd.DataDFrame, pd.Series]:
        pass
class DataPreprocessStrategy(DataStrategy):
    """
    strategy foe preprocessing data
    """
    def handle_data(self, data: pd.DatFrame) -> pd.DatFrame:
        """
        preprocess data
        """
        try:
            data = data.drop_drop([
                "order_approved_at","order_delivery_carrier_date",
                "order_delivered_customer_date",
                "order_estimated_delivery_date",
                "order_purchase_timestamp"
            ], axis=1)
            data["product_weight_g"].fillna(data["product_weight_g"].median(), inplace=True)
            data["product_length_cm"].fillna(data["product_length_cm"].median(), inplace=True)
            data["product_height_cm"].fillna(data["product_height_cm"].median(), inplace=True)
            data["product_width_cm"].fillna(data["product_width_cm"].median(), inplace=True)
            # write "No review" in review_comment_message column
            data["review_comment_message"].fillna("No review", inplace=True)
            data = data.select_dtypes(include=[np.number])
            cols_to_drop = ["customer_zip_code_prefix", "order_item_id"]
            data = data.drop(cols_to_drop, axis=1)

            return data
        except Exception as e:
            logging.error(f"error in preprocessing data: {e}")
            raise e

class DataDivideStrategy(DataStrategy):
    """strategy for divinding the data into test and train """

    def handle_data(self, data:pd.DataFrame)-> Union[pd.DataDFrame, pd.Series]:


        """
    divide the data into test train
        """

        try:

            x = data.drop(["review_score"], axis=1)
            y=data["review_score"]
            x_train, x_test, y_test, y_train= train_test_split(x, y, test_size=0.2, random_state= 42)
            return  x_train,x_test, y_train, y_test
        except Exception as e:
            logging.error(f"error in dividing the date {e}")
            raise e

class DataCleaning:
    """
        class for cleaning the data which processes the data and divides the data INTO TRAIN AND TEST
    """
    def __init__(self, data:pd.DataFrame, strategy: DataStrategy):
        self.data=data
        self.strategy = strategy

    def handle_data(self) -> Union[pd.DataFrame, pd.Series]:
        """Handle data based on the provided strategy"""
        return self.strategy.handle_data(self.df)

# if __name__=="main":
#     data=pd.read_csv("path to datafile")
#     data_cleaning=DataCleaning(data, DataPreprocessStrategy())
#     data_cleaning.handle_data()
