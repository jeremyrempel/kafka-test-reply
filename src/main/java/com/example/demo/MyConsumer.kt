package com.example.demo

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.messaging.handler.annotation.SendTo

@Configuration
class MyConsumer {

    private val logger: Logger = LoggerFactory.getLogger(MyConsumer::class.java)

    @KafkaListener(topics = ["kRequests"])
    @SendTo
    fun listen(record: ConsumerRecord<String, String>, ack: Acknowledgment): String {

        logger.info("received value: ${record.value()}, key: ${record.key()}. thread ${Thread.currentThread()}")
        ack.acknowledge()

        return "OK. FROM CONSUMER. MESSAGE: ${record.value()}"
    }

}