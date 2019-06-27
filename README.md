# java-oracle-exporter

prepare a .env file with
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY

build:
```
docker-compose build
```

### Tests ###
- build the test DB:
     - `git clone https://github.com/wnameless/docker-oracle-xe-11g build/oracle`
     - `docker build -t wnameless/oracle-xe-11g build/oracle`
- run tests
    - `docker-compose run --rm tests`

jar usage:
- export command
outputs file to `output/data.csv`
```
java -jar table-exporter.jar export /path/to/my/configFile.json true
```
- testConnection
returns exit code 0 if connection successful, otherwise returns the exit code of the internal error
```
java -jar table-exporter.jar testConnection /path/to/my/configFile.json
```

- getTables
outputs file to `output/tables.json`
```
java -jar table-exporter.jar getTables /path/to/my/configFile.json
```

arguments:
[0] - action (one of "export", "getTables", "testConnection")
[1] - path to configuration file
[2] - [only valid for export action] boolean includeHeaders
