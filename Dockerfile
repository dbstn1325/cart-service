FROM openjdk:21-ea-11-jdk-slim
VOLUME /tmp
COPY build/libs/cartService-1.0.jar CartService.jar
ENTRYPOINT ["java", "-jar", "CartService.jar"]