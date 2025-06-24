# Roadmap.sh-012
Database Backup for SQLite, PostgreSQL, MongoDB &amp; MySQL with S3 support

#### Requirements
`Python` & `pip`

Note: Dependencies boto3, mysql-connector-python, psycopg[binary] & pymongo shall be `automatically installed on-demand` when you interact with the respective database type.


#### Usage
```
 ./db_backup.py -h
usage: db_backup.py [-h] {init,backup,restore,schedule,list} ...

Database Backup CLI

positional arguments:
  {init,backup,restore,schedule,list}
    init                Initialize backup config
    backup              Perform backup
    restore             Restore database
    schedule            Start scheduler
    list                List targets

options:
  -h, --help            show this help message and exit
```

#### Testing

Inside `/test-data` there's a `docker-compose.yml` that generates `live samples` for the `supported` DBMS and `runs` their respective `servers` on `localhost`. 

Launch the test with `docker compose build --up`.

`S3` support was tested with `docker run -d --name minio --network host -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin quay.io/minio/minio server /data`.

Part of this challenge: https://roadmap.sh/projects/database-backup-utility
