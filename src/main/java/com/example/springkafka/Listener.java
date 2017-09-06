package com.example.springkafka;

import com.example.springkafka.model.Greeting;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;

public class Listener implements ConsumerSeekAware {

    private final ThreadLocal<ConsumerSeekCallback> seekCallBack = new ThreadLocal<>();

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Value(value = "${message.topic.name}")
    private String topicName;

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        this.seekCallBack.set(callback);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {

    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {

    }

    @KafkaListener(id = "greetingListener", topics = "gtopic", containerFactory = "greetingKafkaListenerContainerFactory")
    public void listen(Greeting message) {
        System.out.println("Received Message in group foo: " + message);

    }

    /**
     * Reloading after the set timeout
     *
     * @param event
     */
    @EventListener()
    public void eventHandler(ListenerContainerIdleEvent event) {
        System.out.println("Reloading........");
        this.seekCallBack.get().seek(topicName, 0, 0);
//        kafkaListenerEndpointRegistry.getListenerContainer("greetingListener").stop();
//        kafkaListenerEndpointRegistry.getListenerContainer("greetingListener").start();
    }
}
