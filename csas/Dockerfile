FROM maven:3.5.3-jdk-11-slim

ENV TZ=UTC

WORKDIR /code/
COPY . /code/

COPY docker/TableExporter-keepAlive-jar-with-dependencies.jar /code/TableExporter.jar
COPY docker/config.json /code/config.json

RUN chmod +x /code/TableExporter.jar

RUN java -jar /code/TableExporter.jar export /code/config.json "" true