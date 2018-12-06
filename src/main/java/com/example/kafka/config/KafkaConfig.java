package com.example.kafka.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.validation.constraints.NotNull;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.validation.annotation.Validated;

import com.example.kafka.model.User;

import lombok.Data;

@Data
@Validated
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaConfig {

	private List<TopicConfiguration> topics;

	public Optional<List<TopicConfiguration>> getTopics() {
		return Optional.ofNullable(topics);
	}

	@Bean
	public ProducerFactory<String, User> producerFactory() {
		Map<String, Object> config = new HashMap<>();

		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		return new DefaultKafkaProducerFactory<>(config);
	}

	@Bean
	public KafkaTemplate<String, User> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Data
	static class TopicConfiguration {
		@NotNull(message = "Topic name is required.")
		private String name;
		private Integer numPartitions = 3;
		private Short replicationFactor = 1;

		NewTopic toNewTopic() {
			return new NewTopic(this.name, this.numPartitions, this.replicationFactor);
		}
	}

}
