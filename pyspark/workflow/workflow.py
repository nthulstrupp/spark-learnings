def workflow(spark_context: SQLContext, crunch_datetime: datetime, use_full_data: bool, algo_version: str):

    print("start {}".format(datetime.now()))

    config = CrunchConfiguration(
        crunch_datetime=crunch_datetime)