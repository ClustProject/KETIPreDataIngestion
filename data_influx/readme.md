# influx_Clinet

## class influcClient
- basic influx DB connection

```
getDBList: 
- get all db List according to the influx setting
- remove the 1st useless name (defalut information)
```

### DB Information

```
measurement_list:
- explore the specific database DB with switching database name.
- switch database based on influxClient and the db name
- get all measurement list related to the db
```
```
measurement_list_only_start_end:
- Get the only start and end measurement name, and add the number of ms
- Use this function to reduce the DB load time for visualization.
```

### Measurement Set

```
get_MeasurementDataSet:
- Get measurement Data Set according to the dbinfo
- Each function makes dataframe output with "timedate" index.
-This class get multiple influx measurement information.
```

#### Individual Measurement

```
get_fieldList:
- Get all feature(field) list of the specific measurement.
```
```
get_first_time:
- Get the first data time of the specific mearuement
```

```
get_last_time:
- Get the last data time of the specific mearuement
```

```
get_data:
- Get all data of the specific mearuement
```
```
get_data_by_time:
- Get data of the specific mearuement based on start-end duration
# get_datafront_by_duration(self, start_time, end_time)
    ex> bind_params example
    bind_params = {'end_time': query_end_time.strftime('%Y-%m-%dT%H:%M:%SZ'), 
    'start_time': query_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}
```
```
get_data_by_days:
- Get data of the specific mearuement based on time duration (days)
    ex> bind_param example
        bind_params = {'end_time': 1615991400000, 'days': '7d"}
```

```
get_datafront_by_num:
- Get the first N number data from the specific measurement
```

```
get_dataend_by_num:
- Get the last N number data from the specific measurement
```


```
cleanup_df:
- Clean data, remove duplication, Sort, Set index (datetime)


```
