-- OR condition 

https://stackoverflow.com/questions/40686934/how-to-use-or-condition-in-when-in-spark



-- Renaming of columns 

  .select(col('traffic_type_parent').alias('fv_traffic_type_parent'),
          col('traffic_type_child').alias('fv_traffic_type_child'))

   https://stackoverflow.com/questions/34077353/how-to-change-dataframe-column-names-in-pyspark
   
-- device_type_snippet

     agent_desktop = [1,23,4,5,6,7,11,14]
     agent_phone = [9,10,12]
     agent_tablet = [8,13]

   .withColumn('fv_device_type', when(col('agent_id').isin(agent_desktop),'Desktop/Web')
                                            .when(col('agent_id').isin(agent_phone) & col('is_app').isin(1),'Phone/App')
                                            .when(col('agent_id').isin(agent_tablet) & col('is_app').isin(1),'Tablet/App')
                                            .when(col('agent_id').isin(agent_phone),'Phone/Web')
                                            .when(col('agent_id').isin(agent_tablet),'Tablet/Web')
                                            .otherwise('unknown'))

