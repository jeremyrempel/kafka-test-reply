package com.example.demo

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate


@Configuration
class KafkaConfig {

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()

        val props = mutableMapOf<String, Any>()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ConsumerConfig.GROUP_ID_CONFIG] = "mygroup"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"

        factory.consumerFactory = DefaultKafkaConsumerFactory(props)
        factory.setConcurrency(6)
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
        return factory
    }

    @Bean
    fun producerFactory(): ProducerFactory<String, String> {
        val props: MutableMap<String, Any> = HashMap()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

        return DefaultKafkaProducerFactory(
            // See https://kafka.apache.org/documentation/#producerconfigs for more properties
            props
        )
    }

    @Bean
    fun kafkaTemplate(pf: ProducerFactory<String, String>): KafkaTemplate<String, String?> {
        return KafkaTemplate(pf)
    }

    /**
     *
     */
    @Bean
    fun replyTemplate(
        pf: ProducerFactory<String, String>,
        factory: ConcurrentKafkaListenerContainerFactory<String?, String?>
    ): ReplyingKafkaTemplate<String?, String, String?> {
        val replyContainer = factory.createContainer("kReplies")
//        replyContainer.containerProperties.groupId = "request.replies"
        return ReplyingKafkaTemplate(pf, replyContainer)
    }

    @Bean
    fun repliesContainer(
        containerFactory: ConcurrentKafkaListenerContainerFactory<String, String>,
        kafkaTemplate: KafkaTemplate<String, String?>
    ): ConcurrentMessageListenerContainer<String, String> {

        containerFactory.setReplyTemplate(kafkaTemplate)
        val repliesContainer = containerFactory.createContainer("kReplies")

//        repliesContainer.containerProperties.groupId = "repliesGroup"
        repliesContainer.isAutoStartup = false
        return repliesContainer
    }
}