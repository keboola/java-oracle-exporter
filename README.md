# java-oracle-exporter

download ojdbc8-full http://www.oracle.com/technetwork/database/features/jdbc/jdbc-ucp-122-3110062.html
put in ojdbc8-full

build:
```
docker-compose build
```

run tests:
```
docker-compose run --rm tests
```

jar usage:
```
java -jar table-exporter.jar /path/to/my/configFile.json
```
