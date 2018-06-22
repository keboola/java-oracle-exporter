# java-oracle-exporter

download ojdbc8-full http://www.oracle.com/technetwork/database/features/jdbc/jdbc-ucp-122-3110062.html
put in ojdbc8-full

run with:
```
docker build . -t exporter
docker run -i -t -e KBC_DATADIR=/data/ -v D:\TableExporter\data:/data/ exporter
```
