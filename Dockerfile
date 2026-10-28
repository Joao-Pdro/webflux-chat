# ---- build: compila e empacota (testes rodam no CI, não aqui) ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
# camada separada: só refaz o download das dependências quando o pom muda
COPY pom.xml ./
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q -DskipTests package && mv target/*.jar app.jar

# ---- runtime: só JRE, sem root ----
FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S app && adduser -S app -G app
USER app
WORKDIR /app
COPY --from=build /build/app.jar app.jar
EXPOSE 8080
# heap proporcional ao limite de memória do contêiner
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75"
ENTRYPOINT ["java", "-jar", "app.jar"]
