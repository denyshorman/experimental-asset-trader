FROM bellsoft/liberica-openjdk-alpine:14
ADD ./build/libs/*.jar /app/
WORKDIR /app
CMD java -jar *.jar
