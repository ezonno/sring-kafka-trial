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

public class ScheduledListener implements ConsumerSeekAware {

    private final ThreadLocal<ConsumerSeekCallback> seekCallBack = new ThreadLocal<>();

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Value(value = "${message.topic.name}")
    private String topicName;

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        this.seekCallBack.set(callback);
    }

    /**
     * Reset the offset to the beginning when an partition is assigned, this callback is called when the listenerContainer is started.
     * @param assignments
     * @param callback
     */
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        callback.seekToBeginning(topicName, 0);
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {

    }


    @KafkaListener(id = "greetingListenerScheduled", topics = "gtopic", containerFactory = "scheduledGreetingKafkaListenerContainerFactory")
    public void listenScheduled(Greeting message) {
        System.out.println("Scheduled: Received Message in group foo: " + message);
    }

    /**
     * Reloading after the set timeout
     *
     * @param event
     */
    @EventListener(condition = "event.listenerId.startsWith('greetingListenerScheduled')")
    public void eventHandler(ListenerContainerIdleEvent event) {
        System.out.println("Stopping the scheduled listener");
        kafkaListenerEndpointRegistry.getListenerContainer("greetingListenerScheduled").stop();
    }


    @Scheduled(initialDelay = 10000, fixedDelay = 20000)
    public void doSomething() {
        System.out.println("Starting the scheduled listener");
        kafkaListenerEndpointRegistry.getListenerContainer("greetingListenerScheduled").start();
    }
}
