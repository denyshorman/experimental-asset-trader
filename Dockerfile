FROM openjdk:14-slim-buster
ADD ./build/libs/*.jar /app/
WORKDIR /app
CMD java -jar *.jar
