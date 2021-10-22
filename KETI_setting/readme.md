###
## Define your own Data and MetaData DB information.
## and make influ_setting_KETI.py file under KETI_setting folder.

# It shouold include data DB info.
# we are currently using influx DB.

```json
host_='X.X.X>X'
port_= XXXX
user_='KETI'
pass_='PASSWORD'
protocol ='line'
```

## And it also should include metadata config information
# We are currently using mongo DB.

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