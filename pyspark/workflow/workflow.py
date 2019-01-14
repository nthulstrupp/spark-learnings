from pyspark import SQLContext
from datetime import datetime

from aggregation import read_data, agg_data
from classes import CrunchConfiguration

from insert import insert_overwrite_with_repartition


def workflow(spark_context: SQLContext, crunch_datetime: datetime):

    print("start {}".format(datetime.now()))

    config = CrunchConfiguration(
        crunch_datetime=crunch_datetime)
        
    read_data = read_data(spark_context, config)
    
    agg_data = agg_data(read_data)
    
    insert_overwrite_with_repartition(data_frame=agg_data,
                                      db_name=your_db,
                                      table_name='your_table',
                                      new_partitions=2)
                                      
    print('Workflow ended at: {}'.format(datetime.now()))
