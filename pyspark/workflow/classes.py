from pyspark.sql import SQLContext, DataFrame

from datetime import datetime, timedelta
from functools import partial


class CrunchConfiguration:

    @property
    def crunch_date(self) -> int:
        return int(self.crunch_datetime.strftime('%Y%m%d'))
    
    def __init__(self,
                 crunch_datetime: datetime,
                 ):
        """
        :param crunch_datetime: datetime, crunch datetime
         """
        self.crunch_datetime: datetime = crunch_datetime
        
    def show(self) -> None:
        print(
              ( 'Crunch configuration:'
                ''.join(["\n\t{}: {}".format(k, v) for k, v in list(self.__dict__.items())])
              )
        )
