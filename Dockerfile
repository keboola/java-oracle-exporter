# instead of maven:3.5.0-jdk-8-alpine, contained old java version
FROM maven:3.5.3-jdk-11-slim 

WORKDIR /code/
COPY . /code/
RUN mvn install:install-file -Dfile=/code/ojdbc8-full/ojdbc8.jar -DgroupId=com.oracle.jdbc -DartifactId=ojdbc8 -Dversion=12.2.0.1 -Dpackaging=jar
RUN mvn install

ENTRYPOINT java -jar /code/target/TableExporter-1.0.0-jar-with-dependencies.jar /data
