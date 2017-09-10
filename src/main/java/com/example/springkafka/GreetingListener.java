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
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Map;

public class GreetingListener implements ConsumerSeekAware {

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


    @KafkaListener(id = "greetingListener1", topics = "gtopic", containerFactory = "greetingKafkaListenerContainerFactory")
    public void listenScheduled(Greeting message) {
        System.out.println("Greeting: Received Message in group foo: " + message);
    }

    /**
     * Handle the timeout event and reset the offset to the beginning.
     * The listener is never stopped and was auto started in the beginning.
     *
     * @param event
     */
    @EventListener(condition = "event.listenerId.startsWith('greetingListener1')")
    public void eventHandler(ListenerContainerIdleEvent event) {
        System.out.println("Resetting greeting offset........");
        this.seekCallBack.get().seekToBeginning(topicName, 0);

    }
}
