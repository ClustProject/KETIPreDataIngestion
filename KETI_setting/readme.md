###
# Define your own Data and MetaData DB information.
- make influ_setting_KETI.py file under KETI_setting folder.
- It shouold include data DB info.
- we are currently using influx DB as DATABASE.
# Influx DB Description Example
```json
host_='X.X.X>X'
port_= XXXX
user_='KETI'
pass_='PASSWORD'
protocol ='line'
```

- It also should include metadata config information
- We are currently using mongo DB as the meta DB.

# Mongo DB Description Example
```json
{
    "DB_INFO":{
        "USER_ID":"",
        "USER_PWD":"",
        "HOST_ADDR":"",
        "HOST_PORT":"",
        "DB_NAME":""
    }
}
```
