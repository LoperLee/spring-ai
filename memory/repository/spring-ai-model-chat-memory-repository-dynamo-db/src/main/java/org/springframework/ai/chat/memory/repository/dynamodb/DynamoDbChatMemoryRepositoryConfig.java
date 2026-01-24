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

import java.util.Objects;

import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import org.springframework.util.Assert;

/**
 * Configuration for the DynamoDB Chat Memory store.
 *
 * @author JunSeop Lee
 * @since 1.1.2
 */
public final class DynamoDbChatMemoryRepositoryConfig {

	public static final String DEFAULT_TABLE_NAME = "spring_ai_chat_memory";

	public static final String DEFAULT_PARTITION_KEY_NAME = "conversationId";

	public static final String DEFAULT_SORT_KEY_NAME = "sequenceNumber";

	private final DynamoDbClient dynamoDbClient;

	private final String tableName;

	private final String partitionKeyName;

	private final String sortKeyName;

	private DynamoDbChatMemoryRepositoryConfig(Builder builder) {
		this.dynamoDbClient = Objects.requireNonNull(builder.dynamoDbClient);
		this.tableName = builder.tableName;
		this.partitionKeyName = builder.partitionKeyName;
		this.sortKeyName = builder.sortKeyName;
		this.initializeTable();
	}

	public static Builder builder() {
		return new Builder();
	}

	public DynamoDbClient getDynamoDbClient() {
		return this.dynamoDbClient;
	}

	public String getTableName() {
		return this.tableName;
	}

	public String getPartitionKeyName() {
		return this.partitionKeyName;
	}

	public String getSortKeyName() {
		return this.sortKeyName;
	}

	private void initializeTable() {
		try {
			this.dynamoDbClient.describeTable(DescribeTableRequest.builder().tableName(this.tableName).build());
			return;
		}
		catch (ResourceNotFoundException ex) {
			// Table does not exist yet.
		}

		CreateTableRequest request = CreateTableRequest.builder()
			.tableName(this.tableName)
			.attributeDefinitions(AttributeDefinition.builder()
				.attributeName(this.partitionKeyName)
				.attributeType(ScalarAttributeType.S)
				.build(),
					AttributeDefinition.builder()
						.attributeName(this.sortKeyName)
						.attributeType(ScalarAttributeType.N)
						.build())
			.keySchema(KeySchemaElement.builder().attributeName(this.partitionKeyName).keyType(KeyType.HASH).build(),
					KeySchemaElement.builder().attributeName(this.sortKeyName).keyType(KeyType.RANGE).build())
			.billingMode(BillingMode.PAY_PER_REQUEST)
			.build();

		this.dynamoDbClient.createTable(request);

		DynamoDbWaiter waiter = this.dynamoDbClient.waiter();
		waiter.waitUntilTableExists(DescribeTableRequest.builder().tableName(this.tableName).build());
	}

	public static final class Builder {

		private @Nullable DynamoDbClient dynamoDbClient;

		private String tableName = DEFAULT_TABLE_NAME;

		private String partitionKeyName = DEFAULT_PARTITION_KEY_NAME;

		private String sortKeyName = DEFAULT_SORT_KEY_NAME;

		private Builder() {
		}

		public Builder withDynamoDbClient(DynamoDbClient dynamoDbClient) {
			this.dynamoDbClient = dynamoDbClient;
			return this;
		}

		public Builder withTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		public Builder withPartitionKeyName(String partitionKeyName) {
			this.partitionKeyName = partitionKeyName;
			return this;
		}

		public Builder withSortKeyName(String sortKeyName) {
			this.sortKeyName = sortKeyName;
			return this;
		}

		public DynamoDbChatMemoryRepositoryConfig build() {
			Assert.notNull(this.dynamoDbClient, "DynamoDbClient cannot be null");
			Assert.hasText(this.tableName, "tableName cannot be null or empty");
			Assert.hasText(this.partitionKeyName, "partitionKeyName cannot be null or empty");
			Assert.hasText(this.sortKeyName, "sortKeyName cannot be null or empty");

			return new DynamoDbChatMemoryRepositoryConfig(this);
		}

	}

}
