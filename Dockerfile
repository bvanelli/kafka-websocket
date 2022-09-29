FROM maven:3-openjdk-18
ADD ./ /opt/kafka-websocket
WORKDIR /opt/kafka-websocket
RUN mvn clean package -Dmaven.test.skip
CMD java -jar target/kafka-websocket-0.8.3-SNAPSHOT-shaded.jar
