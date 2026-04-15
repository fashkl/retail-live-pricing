package com.retail.livepricing.common.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@EnableConfigurationProperties({KafkaTopicsProperties.class, AppProperties.class})
public class KafkaTopicConfig {

    @Bean
    NewTopic priceTicksTopic(KafkaTopicsProperties props) {
        return TopicBuilder.name(props.priceTicks()).partitions(12).replicas(1).build();
    }

    @Bean
    NewTopic priceUpdatesTopic(KafkaTopicsProperties props) {
        return TopicBuilder.name(props.priceUpdates()).partitions(12).replicas(1).build();
    }

    @Bean
    NewTopic portfolioCalcTasksTopic(KafkaTopicsProperties props) {
        return TopicBuilder.name(props.portfolioCalcTasks()).partitions(12).replicas(1).build();
    }

    @Bean
    NewTopic portfolioUpdatesTopic(KafkaTopicsProperties props) {
        return TopicBuilder.name(props.portfolioUpdates()).partitions(12).replicas(1).build();
    }

    @Bean
    NewTopic deadLetterTopic(KafkaTopicsProperties props) {
        return TopicBuilder.name(props.deadLetter()).partitions(6).replicas(1).build();
    }
}
