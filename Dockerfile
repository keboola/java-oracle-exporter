FROM quay.io/keboola/aws-cli
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_ACCESS_KEY_ID
ARG AWS_SESSION_TOKEN
RUN /usr/bin/aws s3 cp s3://keboola-drivers/ojdbc8-full/ojdbc8.jar /tmp/ojdbc8-full/ojdbc8.jar

# instead of maven:3.5.0-jdk-8-alpine, contained old java version
FROM maven:3.5.3-jdk-11-slim 

ENV TZ=UTC

WORKDIR /code/
COPY . /code/
COPY --from=0 /tmp/ojdbc8-full/ojdbc8.jar /code/ojdbc8-full/ojdbc8.jar
RUN mvn install:install-file -Dfile=/code/ojdbc8-full/ojdbc8.jar -DgroupId=com.oracle.jdbc -DartifactId=ojdbc8 -Dversion=12.2.0.1 -Dpackaging=jar
RUN mvn clean verify -DskipTests
