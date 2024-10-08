FROM eclipse-temurin:17-jdk-alpine
VOLUME /tmp
COPY target/*.jar app.jar
COPY target/classes/tpch.db /tpch-tiny.db
ENTRYPOINT ["java", "--add-opens=java.base/java.nio=ALL-UNNAMED", "-jar", "/app.jar"]
EXPOSE 8080