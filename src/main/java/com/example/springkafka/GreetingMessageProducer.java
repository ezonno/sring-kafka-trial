package com.example.springkafka;

import com.example.springkafka.model.Greeting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class GreetingMessageProducer implements CommandLineRunner {

    private final Logger logger = LoggerFactory.getLogger(GreetingMessageProducer.class);

    @Value(value = "${message.topic.name}")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, Greeting> kafkaTemplate;

    public GreetingMessageProducer(KafkaTemplate<String, Greeting> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void run(String... strings) throws Exception {
        logger.info("Initial sending greeting messages");

        for (int i=0; i < 20; i++) {
            kafkaTemplate.send(topicName, new Greeting("name" + i, "message" + i));
        }
    }
}
