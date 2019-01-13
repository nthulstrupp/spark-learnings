from datetime import datetime
from pyspark.sql import DataFrame

def insert_overwrite_small_table(data_frame: DataFrame, db_name: str, table_name: str) -> None:
    insert_overwrite_with_repartition(data_frame, db_name, table_name, new_partitions=1)


def insert_overwrite_with_repartition(data_frame: DataFrame, db_name: str, table_name: str, new_partitions: int):
    data_frame_repartitioned = data_frame.repartition(new_partitions)
    insert_overwrite_without_repartition(
        data_frame=data_frame_repartitioned,
        db_name=db_name,
        table_name=table_name)


def insert_overwrite_without_repartition(data_frame: DataFrame, db_name: str, table_name: str):
    print(
        'Started writing table {db_name}.{table_name} at {time_stamp}'
            .format(db_name=db_name, table_name=table_name, time_stamp=datetime.now())
    )

    data_frame.write.insertInto('{db_name}.{table_name}'.format(db_name=db_name, table_name=table_name), overwrite=True)

    print(
        'Finished writing table {db_name}.{table_name} at {time_stamp}'
            .format(db_name=db_name, table_name=table_name, time_stamp=datetime.now())
    )