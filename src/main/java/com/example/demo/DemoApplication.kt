package com.example.demo

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate
import org.springframework.kafka.support.SendResult
import java.time.Duration
import java.util.concurrent.TimeUnit


@SpringBootApplication
class DemoApplication {

    private val logger = LoggerFactory.getLogger(DemoApplication::class.java);

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplication.run(DemoApplication::class.java, *args)
        }
    }

    @Bean
    fun runner(template: ReplyingKafkaTemplate<String?, String?, String>): ApplicationRunner {
        return ApplicationRunner {
            check(template.waitForAssignment(Duration.ofSeconds(10))) { "Reply container did not initialize" }
            val record = ProducerRecord<String?, String?>("kRequests", "foo")
            val replyFuture = template.sendAndReceive(record)

            val sendResult: SendResult<String?, String?> = replyFuture.sendFuture[10, TimeUnit.SECONDS]
            logger.info("Sent ok: " + sendResult.recordMetadata)

            val consumerRecord = replyFuture[10, TimeUnit.SECONDS]
            logger.info("Return value: " + consumerRecord.value())
        }
    }

    @Bean
    fun kRequests(): NewTopic? {
        return TopicBuilder.name("kRequests")
            .partitions(10)
//            .replicas(2)
            .build()
    }

    @Bean
    fun kReplies(): NewTopic? {
        return TopicBuilder.name("kReplies")
            .partitions(10)
//            .replicas(2)
            .build()
    }
}