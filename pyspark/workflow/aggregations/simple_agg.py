from pyspark.sql import SQLContext, DataFrame
from pyspark.sql.functions import col, lit

from classes import CrunchConfiguration


def read_data(spark_context: SQLContext, configuration: CrunchConfiguration) -> DataFrame:
    query = '''
      SELECT
        ymd AS ymd,
		grouping as group,
        id as id,
		price
      FROM
        your_table
      WHERE
        date = {crunch_date}
    '''.format(crunch_date=configuration.crunch_date)
    return spark_context.sql(query)
	
	

	
def agg_data(read_data: DataFrame) -> DataFrame:
    
    agg_data_out = (
        read_data
            .groupby(['grouping'])
            .agg(sum_(when(col('id') == 1, col('price')).otherwise(0.)).cast(DecimalType()).alias('price_data')
                )
    )
    
    
    return agg_data_out