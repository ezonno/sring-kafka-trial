package com.example.springkafka;

import com.example.springkafka.model.Greeting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListener;

import java.util.List;

@SpringBootApplication
public class SpringKafkaApplication {

	public static void main(String[] args) {
        new SpringApplicationBuilder(SpringKafkaApplication.class)
				.web(false)
				.run(args);
	}
 }
