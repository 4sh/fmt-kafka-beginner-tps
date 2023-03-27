FROM eclipse-temurin:17-jdk-jammy as build

WORKDIR /app
COPY .mvn/ .mvn
COPY mvnw pom.xml ./

RUN ./mvnw dependency:resolve

COPY src ./src

RUN ./mvnw clean package


FROM build AS run

COPY --from=build /app/target/generic-avro-producer-1.0-SNAPSHOT-jar-with-dependencies.jar generic-avro-producer.jar
COPY resources resources

ENTRYPOINT ["java", "-jar", "generic-avro-producer.jar"]
