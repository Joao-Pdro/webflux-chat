package dev.joao.chat;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.mongodb.MongoDBContainer;

/** Mongo descartável; o @ServiceConnection substitui a URI do application.properties. */
@TestConfiguration(proxyBeanMethods = false)
public class MongoContainerConfig {

    @Bean
    @ServiceConnection
    MongoDBContainer mongo() {
        return new MongoDBContainer("mongo:9");
    }
}
