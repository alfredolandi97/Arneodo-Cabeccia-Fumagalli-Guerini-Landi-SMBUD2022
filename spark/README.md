# Spark assignment
This folder contains all scripts and source .csv file that have been used for the third assignment. Following is a quick recap:
- [pre_processing](./spark/pre_processing), contains the scripts used to convert the [dataset](./spark/pre_processing/sample_data) of the first assignment:
  - _dataset_preprocessing.py_ splits the dataset according to the spark model
  - _author_editor_mod.py_, further adds fields to author and editor
- [resources](./spark/resources), contains the processed dataset in both header and headerless version
- [SparkInitializer.py](./spark/SparkInitializer.py), python class for dataframes management
- [insert.py](./spark/insert.py), insert, update and delete queries
- [queries.py](./spark/queries.py), the ten requested queries
