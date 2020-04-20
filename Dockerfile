FROM bellsoft/liberica-openjdk-alpine:14
ADD ./build/libs/*.jar /app/
CMD ["/usr/lib/jvm/jdk/bin/java", "-jar", "/app/app.jar"]
