# instead of maven:3.5.0-jdk-8-alpine, contained old java version
FROM maven:3.5.3-jdk-11-slim

ENV TZ=UTC

WORKDIR /code/
COPY . /code/
RUN mvn clean verify -DskipTests
