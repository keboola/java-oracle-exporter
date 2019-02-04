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
java -jar table-exporter.jar export /path/to/my/configFile.json
```

arguments:
[0] - action (one of "export", "getTables", "testConnecgion")
[1] - [only valid for export action] path to configuration file
[2] - [only valid for export action] boolean includeHeaders
