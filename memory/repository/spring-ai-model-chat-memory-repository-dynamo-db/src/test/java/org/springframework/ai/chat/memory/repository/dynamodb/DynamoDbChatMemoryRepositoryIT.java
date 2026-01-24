/*
 * Copyright 2023-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.ai.chat.memory.repository.dynamodb;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.MessageType;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link DynamoDbChatMemoryRepository}.
 *
 * @author JunSeop Lee
 * @since 1.1.2
 */
@Testcontainers
class DynamoDbChatMemoryRepositoryIT {

	private static final String TABLE_NAME = "spring-ai-chat-memory-" + UUID.randomUUID();

	@Container
	static GenericContainer<?> dynamoDbContainer = new GenericContainer<>("amazon/dynamodb-local:latest")
		.withExposedPorts(8000)
		.waitingFor(Wait.forListeningPort());

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
		.withUserConfiguration(DynamoDbChatMemoryRepositoryIT.TestApplication.class);

	@Test
	void ensureBeansGetsCreated() {
		this.contextRunner.run(context -> {
			DynamoDbChatMemoryRepository memory = context.getBean(DynamoDbChatMemoryRepository.class);
			Assertions.assertNotNull(memory);
		});
	}

	@ParameterizedTest
	@CsvSource({ "Message from assistant,ASSISTANT", "Message from user,USER", "Message from system,SYSTEM" })
	void add_shouldInsertSingleMessage(String content, MessageType messageType) {
		this.contextRunner.run(context -> {
			var chatMemoryRepository = context.getBean(ChatMemoryRepository.class);
			var conversationId = UUID.randomUUID().toString();
			var message = switch (messageType) {
				case ASSISTANT -> new AssistantMessage(content);
				case USER -> new UserMessage(content);
				case SYSTEM -> new SystemMessage(content);
				default -> throw new IllegalArgumentException("Type not supported: " + messageType);
			};

			chatMemoryRepository.saveAll(conversationId, List.of(message));
			assertThat(chatMemoryRepository.findConversationIds()).isNotEmpty();
			assertThat(chatMemoryRepository.findConversationIds()).contains(conversationId);

			List<Message> retrievedMessages = chatMemoryRepository.findByConversationId(conversationId);
			assertThat(retrievedMessages).hasSize(1);
			assertThat(retrievedMessages.get(0).getText()).isEqualTo(content);
			assertThat(retrievedMessages.get(0).getMessageType()).isEqualTo(messageType);
		});
	}

	@Test
	void shouldSaveAndRetrieveMultipleMessages() {
		this.contextRunner.run(context -> {
			var chatMemoryRepository = context.getBean(ChatMemoryRepository.class);
			var conversationId = UUID.randomUUID().toString();

			List<Message> messages = List.of(new SystemMessage("System message"), new UserMessage("User message"),
					new AssistantMessage("Assistant message"));

			chatMemoryRepository.saveAll(conversationId, messages);

			List<Message> retrievedMessages = chatMemoryRepository.findByConversationId(conversationId);
			assertThat(retrievedMessages).hasSize(3);

			assertThat(retrievedMessages.get(0).getText()).isEqualTo("System message");
			assertThat(retrievedMessages.get(0).getMessageType()).isEqualTo(MessageType.SYSTEM);

			assertThat(retrievedMessages.get(1).getText()).isEqualTo("User message");
			assertThat(retrievedMessages.get(1).getMessageType()).isEqualTo(MessageType.USER);

			assertThat(retrievedMessages.get(2).getText()).isEqualTo("Assistant message");
			assertThat(retrievedMessages.get(2).getMessageType()).isEqualTo(MessageType.ASSISTANT);
		});
	}

	@Test
	void shouldReplaceExistingMessages() {
		this.contextRunner.run(context -> {
			var chatMemoryRepository = context.getBean(ChatMemoryRepository.class);
			var conversationId = UUID.randomUUID().toString();

			List<Message> initialMessages = List.of(new UserMessage("Initial user message"),
					new AssistantMessage("Initial assistant message"));
			chatMemoryRepository.saveAll(conversationId, initialMessages);

			List<Message> retrievedMessages = chatMemoryRepository.findByConversationId(conversationId);
			assertThat(retrievedMessages).hasSize(2);

			List<Message> newMessages = List.of(new SystemMessage("New system message"),
					new UserMessage("New user message"));
			chatMemoryRepository.saveAll(conversationId, newMessages);

			retrievedMessages = chatMemoryRepository.findByConversationId(conversationId);
			assertThat(retrievedMessages).hasSize(2);
			assertThat(retrievedMessages.get(0).getText()).isEqualTo("New system message");
			assertThat(retrievedMessages.get(1).getText()).isEqualTo("New user message");
		});
	}

	@Test
	void shouldDeleteConversation() {
		this.contextRunner.run(context -> {
			var chatMemoryRepository = context.getBean(ChatMemoryRepository.class);
			var conversationId = UUID.randomUUID().toString();

			List<Message> messages = List.of(new UserMessage("User message"),
					new AssistantMessage("Assistant message"));
			chatMemoryRepository.saveAll(conversationId, messages);

			assertThat(chatMemoryRepository.findByConversationId(conversationId)).hasSize(2);

			chatMemoryRepository.deleteByConversationId(conversationId);

			assertThat(chatMemoryRepository.findByConversationId(conversationId)).isEmpty();
		});
	}

	@Test
	void shouldFindAllConversationIds() {
		this.contextRunner.run(context -> {
			var chatMemoryRepository = context.getBean(ChatMemoryRepository.class);
			var conversationId1 = UUID.randomUUID().toString();
			var conversationId2 = UUID.randomUUID().toString();

			chatMemoryRepository.saveAll(conversationId1, List.of(new UserMessage("Message 1")));
			chatMemoryRepository.saveAll(conversationId2, List.of(new UserMessage("Message 2")));

			List<String> conversationIds = chatMemoryRepository.findConversationIds();
			assertThat(conversationIds).contains(conversationId1, conversationId2);
		});
	}

	@Test
	void shouldHandleEmptyConversation() {
		this.contextRunner.run(context -> {
			var chatMemoryRepository = context.getBean(ChatMemoryRepository.class);
			var conversationId = UUID.randomUUID().toString();

			List<Message> messages = chatMemoryRepository.findByConversationId(conversationId);
			assertThat(messages).isEmpty();

			chatMemoryRepository.deleteByConversationId(conversationId);
		});
	}

	@SpringBootConfiguration
	@EnableAutoConfiguration
	static class TestApplication {

		@Bean
		public DynamoDbClient dynamoDbClient() {
			String endpoint = "http://" + dynamoDbContainer.getHost() + ":" + dynamoDbContainer.getMappedPort(8000);
			return DynamoDbClient.builder()
				.endpointOverride(URI.create(endpoint))
				.region(Region.US_EAST_1)
				.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
				.build();
		}

		@Bean
		public DynamoDbChatMemoryRepositoryConfig dynamoDbChatMemoryRepositoryConfig(DynamoDbClient dynamoDbClient) {
			return DynamoDbChatMemoryRepositoryConfig.builder()
				.withDynamoDbClient(dynamoDbClient)
				.build();
		}

		@Bean
		public DynamoDbChatMemoryRepository dynamoDbChatMemoryRepository(DynamoDbChatMemoryRepositoryConfig config) {
			return DynamoDbChatMemoryRepository.create(config);
		}

	}

}
