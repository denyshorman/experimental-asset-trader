FROM bellsoft/liberica-openjdk-alpine:14
ADD ./build/libs/app.jar /app/
CMD ["/usr/lib/jvm/jre/java", "-jar", "/app/app.jar"]
