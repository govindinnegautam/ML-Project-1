import os
from mlProject import logger
from mlProject.entity.config_entity import DataValidationConfig
import pandas as pd

class DataValidation:
    def __init__(self,config: DataValidationConfig):
        self.config = config

    def validate_all_columns(self) -> bool:
        try:
            validation_status = None

            data = pd.read_csv(self.config.unzip_data_dir)
            all_cols = list(data.columns)

            all_schema = self.config.all_schema

            for col in all_cols:
                if col not in all_schema:
                    validation_status = False
                    with open(self.config.STATUS_FILE, 'w') as f:
                        f.write(f"Validation status : {validation_status}\n")
                else:
                    validation_status = True
                    with open(self.config.STATUS_FILE, 'w') as f:
                        f.write(f"Validation status : {validation_status}\n")
                return validation_status
             
            for col, expected_type in all_schema.items():

                if col not in data.columns:
                    validation_status = False
                    with open(self.config.STATUS_FILE, 'w') as f:
                        f.write(f"Missing required column: {col}\n")
                        f.write(f"Validation status : {validation_status}\n")
                    return validation_status

                actual_dtype = str(data[col].dtype)  

               
                if expected_type.lower() == "float":
                    if actual_dtype != "float64":
                        validation_status = False
                        with open(self.config.STATUS_FILE, 'w') as f:
                            f.write(f"Wrong dtype for '{col}'\n")
                            f.write(f"Expected: float64, Got: {actual_dtype}\n")
                            f.write(f"Validation status : {validation_status}\n")
                        return validation_status

                
                if expected_type.lower() == "int":
                    if actual_dtype != "int64":
                        validation_status = False
                        with open(self.config.STATUS_FILE, 'w') as f:
                            f.write(f"Wrong dtype for '{col}'\n")
                            f.write(f"Expected: int64, Got: {actual_dtype}\n")
                            f.write(f"Validation status : {validation_status}\n")
                        return validation_status

           
            with open(self.config.STATUS_FILE, 'w') as f:
                f.write(f"Validation status : {validation_status}\n")

            return validation_status

        except Exception as e:
            raise e