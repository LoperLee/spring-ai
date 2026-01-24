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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.MessageType;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.ToolResponseMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.util.Assert;

/**
 * An implementation of {@link ChatMemoryRepository} for AWS DynamoDB.
 *
 * @author JunSeop Lee
 * @since 1.1.2
 */
public final class DynamoDbChatMemoryRepository implements ChatMemoryRepository {

	public static final String CONVERSATION_TS = DynamoDbChatMemoryRepository.class.getSimpleName()
			+ "_message_timestamp";

	private static final Logger logger = LoggerFactory.getLogger(DynamoDbChatMemoryRepository.class);

	private static final String MESSAGE_TYPE_ATTRIBUTE = "messageType";

	private static final String CONTENT_ATTRIBUTE = "content";

	private static final String MESSAGE_TIMESTAMP_ATTRIBUTE = "messageTimestamp";

	private static final String METADATA_ATTRIBUTE = "metadata";

	private final DynamoDbClient dynamoDbClient;

	private final String tableName;

	private final String partitionKeyName;

	private final String sortKeyName;

	private DynamoDbChatMemoryRepository(DynamoDbChatMemoryRepositoryConfig config) {
		Assert.notNull(config, "config cannot be null");
		this.dynamoDbClient = config.getDynamoDbClient();
		this.tableName = config.getTableName();
		this.partitionKeyName = config.getPartitionKeyName();
		this.sortKeyName = config.getSortKeyName();
	}

	public static DynamoDbChatMemoryRepository create(DynamoDbChatMemoryRepositoryConfig config) {
		return new DynamoDbChatMemoryRepository(config);
	}

	@Override
	public List<String> findConversationIds() {
		logger.info("Finding all conversation IDs from DynamoDB");

		ScanRequest request = ScanRequest.builder()
			.tableName(this.tableName)
			.projectionExpression("#pk")
			.expressionAttributeNames(Map.of("#pk", this.partitionKeyName))
			.build();

		Set<String> conversationIds = new LinkedHashSet<>();
		this.dynamoDbClient.scanPaginator(request).items().forEach(item -> {
			AttributeValue value = item.get(this.partitionKeyName);
			if (value != null && value.s() != null) {
				conversationIds.add(value.s());
			}
		});

		return new ArrayList<>(conversationIds);
	}

	@Override
	public List<Message> findByConversationId(String conversationId) {
		Assert.hasText(conversationId, "conversationId cannot be null or empty");
		logger.info("Finding messages for conversation: {}", conversationId);

		QueryRequest request = QueryRequest.builder()
			.tableName(this.tableName)
			.keyConditionExpression("#pk = :conversationId")
			.expressionAttributeNames(Map.of("#pk", this.partitionKeyName))
			.expressionAttributeValues(
					Map.of(":conversationId", AttributeValue.builder().s(conversationId).build()))
			.scanIndexForward(true)
			.build();

		List<Map<String, AttributeValue>> items = new ArrayList<>();
		this.dynamoDbClient.queryPaginator(request).items().forEach(items::add);
		if (items.isEmpty()) {
			return Collections.emptyList();
		}

		return items.stream().map(this::mapToMessage).collect(Collectors.toList());
	}

	@Override
	public void saveAll(String conversationId, List<Message> messages) {
		Assert.hasText(conversationId, "conversationId cannot be null or empty");
		Assert.notNull(messages, "messages cannot be null");
		Assert.noNullElements(messages, "messages cannot contain null elements");

		logger.info("Saving {} messages for conversation: {}", messages.size(), conversationId);

		deleteByConversationId(conversationId);

		Instant timestamp = Instant.now();
		for (int i = 0; i < messages.size(); i++) {
			Message message = messages.get(i);
			Map<String, AttributeValue> item = createMessageItem(conversationId, message, timestamp, i);
			PutItemRequest request = PutItemRequest.builder().tableName(this.tableName).item(item).build();
			this.dynamoDbClient.putItem(request);
		}
	}

	@Override
	public void deleteByConversationId(String conversationId) {
		Assert.hasText(conversationId, "conversationId cannot be null or empty");
		logger.info("Deleting messages for conversation: {}", conversationId);

		QueryRequest request = QueryRequest.builder()
			.tableName(this.tableName)
			.keyConditionExpression("#pk = :conversationId")
			.expressionAttributeNames(Map.of("#pk", this.partitionKeyName, "#sk", this.sortKeyName))
			.expressionAttributeValues(
					Map.of(":conversationId", AttributeValue.builder().s(conversationId).build()))
			.projectionExpression("#sk")
			.build();

		List<Map<String, AttributeValue>> items = new ArrayList<>();
		this.dynamoDbClient.queryPaginator(request).items().forEach(items::add);
		for (Map<String, AttributeValue> item : items) {
			AttributeValue sortKeyValue = item.get(this.sortKeyName);
			if (sortKeyValue == null) {
				continue;
			}

			Map<String, AttributeValue> key = new HashMap<>();
			key.put(this.partitionKeyName, AttributeValue.builder().s(conversationId).build());
			key.put(this.sortKeyName, sortKeyValue);

			this.dynamoDbClient.deleteItem(
					DeleteItemRequest.builder().tableName(this.tableName).key(key).build());
		}
	}

	private Map<String, AttributeValue> createMessageItem(String conversationId, Message message, Instant timestamp,
			int sequenceNumber) {
		Map<String, AttributeValue> item = new HashMap<>();
		item.put(this.partitionKeyName, AttributeValue.builder().s(conversationId).build());
		item.put(this.sortKeyName, AttributeValue.builder().n(Integer.toString(sequenceNumber)).build());
		item.put(MESSAGE_TYPE_ATTRIBUTE, AttributeValue.builder().s(message.getMessageType().name()).build());
		if (message.getText() != null) {
			item.put(CONTENT_ATTRIBUTE, AttributeValue.builder().s(message.getText()).build());
		}

		Instant messageTimestamp = (Instant) message.getMetadata().get(CONVERSATION_TS);
		if (messageTimestamp == null) {
			messageTimestamp = timestamp;
			message.getMetadata().put(CONVERSATION_TS, messageTimestamp);
		}
		item.put(MESSAGE_TIMESTAMP_ATTRIBUTE,
				AttributeValue.builder().n(Long.toString(messageTimestamp.toEpochMilli())).build());

		Map<String, Object> filteredMetadata = message.getMetadata()
			.entrySet()
			.stream()
			.filter(entry -> !CONVERSATION_TS.equals(entry.getKey()))
			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		if (!filteredMetadata.isEmpty()) {
			item.put(METADATA_ATTRIBUTE, AttributeValue.builder().m(toAttributeValueMap(filteredMetadata)).build());
		}

		return item;
	}

	private Message mapToMessage(Map<String, AttributeValue> item) {
		AttributeValue contentAttribute = item.get(CONTENT_ATTRIBUTE);
		String content = Objects.requireNonNull(contentAttribute, "content attribute is required").s();

		AttributeValue messageTypeAttribute = item.get(MESSAGE_TYPE_ATTRIBUTE);
		String messageTypeStr = Objects.requireNonNull(messageTypeAttribute, "messageType attribute is required").s();
		MessageType messageType = MessageType.valueOf(messageTypeStr);

		Map<String, Object> metadata = new HashMap<>();
		AttributeValue timestampAttribute = item.get(MESSAGE_TIMESTAMP_ATTRIBUTE);
		if (timestampAttribute != null && timestampAttribute.n() != null) {
			metadata.put(CONVERSATION_TS, Instant.ofEpochMilli(Long.parseLong(timestampAttribute.n())));
		}

		AttributeValue metadataAttribute = item.get(METADATA_ATTRIBUTE);
		if (metadataAttribute != null && metadataAttribute.m() != null) {
			metadata.putAll(fromAttributeValueMap(metadataAttribute.m()));
		}

		return switch (messageType) {
			case ASSISTANT -> AssistantMessage.builder().content(content).properties(metadata).build();
			case USER -> UserMessage.builder().text(content).metadata(metadata).build();
			case SYSTEM -> SystemMessage.builder().text(content).metadata(metadata).build();
			case TOOL -> ToolResponseMessage.builder().responses(List.of()).metadata(metadata).build();
			default -> throw new IllegalStateException(String.format("Unknown message type: %s", messageTypeStr));
		};
	}

	private Map<String, AttributeValue> toAttributeValueMap(Map<String, Object> metadata) {
		Map<String, AttributeValue> converted = new HashMap<>();
		for (Map.Entry<String, Object> entry : metadata.entrySet()) {
			converted.put(entry.getKey(), toAttributeValue(entry.getValue()));
		}
		return converted;
	}

	private AttributeValue toAttributeValue(Object value) {
		if (value == null) {
			return AttributeValue.builder().nul(true).build();
		}
		if (value instanceof String stringValue) {
			return AttributeValue.builder().s(stringValue).build();
		}
		if (value instanceof Number numberValue) {
			return AttributeValue.builder().n(numberValue.toString()).build();
		}
		if (value instanceof Boolean booleanValue) {
			return AttributeValue.builder().bool(booleanValue).build();
		}
		if (value instanceof Instant instantValue) {
			return AttributeValue.builder().n(Long.toString(instantValue.toEpochMilli())).build();
		}
		if (value instanceof List<?> listValue) {
			List<AttributeValue> values = listValue.stream().map(this::toAttributeValue).toList();
			return AttributeValue.builder().l(values).build();
		}
		if (value instanceof Map<?, ?> mapValue) {
			Map<String, AttributeValue> converted = new HashMap<>();
			for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
				if (entry.getKey() instanceof String key) {
					converted.put(key, toAttributeValue(entry.getValue()));
				}
			}
			return AttributeValue.builder().m(converted).build();
		}
		return AttributeValue.builder().s(value.toString()).build();
	}

	private Map<String, Object> fromAttributeValueMap(Map<String, AttributeValue> metadata) {
		Map<String, Object> converted = new HashMap<>();
		for (Map.Entry<String, AttributeValue> entry : metadata.entrySet()) {
			converted.put(entry.getKey(), entry.getValue().s());
		}
		return converted;
	}

}
