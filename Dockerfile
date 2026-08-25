FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
# Interview mein **short mein** aise bolna:

#  “This is a multi-stage Dockerfile. First stage mein Maven aur JDK 21 use karke Spring Boot application build karta hoon. Dependencies pehle download karta hoon, phir source code copy karke JAR banata hoon. Second stage mein sirf lightweight JRE 21 image use karke generated JAR run karta hoon. Isse final Docker image chhoti aur production-friendly rehti hai.”
