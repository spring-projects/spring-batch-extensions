/*
 * Copyright 2024-2025 the original author or authors.
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
package org.springframework.batch.extensions.notion;

import org.jspecify.annotations.Nullable;
import org.springframework.batch.extensions.notion.mapping.PropertyMapper;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.util.Assert;

/**
 * A builder for the {@link NotionDatabaseItemReader}.
 *
 * @param <T> Type of item to be read
 * @author Jaeung Ha
 * @see NotionDatabaseItemReader
 */
public class NotionDatabaseItemReaderBuilder<T> {

	private static final int DEFAULT_PAGE_SIZE = 100;

	private @Nullable String token;

	private @Nullable String databaseId;

	private @Nullable PropertyMapper<T> propertyMapper;

	private @Nullable String baseUrl;

	private @Nullable Filter filter;

	private @Nullable String name;

	private Sort[] sorts = new Sort[0];

	private int pageSize = DEFAULT_PAGE_SIZE;

	private boolean saveState = true;

	private int maxItemCount = Integer.MAX_VALUE;

	private int currentItemCount = 0;

	/**
	 * Sets the Notion integration token.
	 * @param token the token
	 * @return this builder
	 * @see NotionDatabaseItemReader#NotionDatabaseItemReader(String, String,
	 * PropertyMapper)
	 */
	public NotionDatabaseItemReaderBuilder<T> token(String token) {
		this.token = token;
		return this;
	}

	/**
	 * Sets the UUID of the database to read from.
	 * @param databaseId the database UUID
	 * @return this builder
	 * @see NotionDatabaseItemReader#NotionDatabaseItemReader(String, String,
	 * PropertyMapper)
	 */
	public NotionDatabaseItemReaderBuilder<T> databaseId(String databaseId) {
		this.databaseId = databaseId;
		return this;
	}

	/**
	 * Sets the {@link PropertyMapper} to use.
	 * @param propertyMapper the property mapper
	 * @return this builder
	 * @see NotionDatabaseItemReader#NotionDatabaseItemReader(String, String,
	 * PropertyMapper)
	 */
	public NotionDatabaseItemReaderBuilder<T> propertyMapper(PropertyMapper<T> propertyMapper) {
		this.propertyMapper = propertyMapper;
		return this;
	}

	/**
	 * Sets the base URL of the Notion API.
	 * @param baseUrl the base URL
	 * @return this builder
	 * @see NotionDatabaseItemReader#setBaseUrl(String)
	 */
	public NotionDatabaseItemReaderBuilder<T> baseUrl(String baseUrl) {
		this.baseUrl = baseUrl;
		return this;
	}

	/**
	 * Sets the {@link Filter} to apply.
	 * @param filter the filter
	 * @return this builder
	 * @see NotionDatabaseItemReader#setFilter(Filter)
	 */
	public NotionDatabaseItemReaderBuilder<T> filter(Filter filter) {
		this.filter = filter;
		return this;
	}

	/**
	 * Sets the {@link Sort}s to apply.
	 * @param sorts the sorts
	 * @return this builder
	 * @see NotionDatabaseItemReader#setSorts(Sort...)
	 */
	public NotionDatabaseItemReaderBuilder<T> sorts(Sort... sorts) {
		this.sorts = sorts;
		return this;
	}

	/**
	 * Sets the number of items to be read with each page.
	 * @param pageSize the page size
	 * @return this builder
	 * @see NotionDatabaseItemReader#setPageSize(int)
	 */
	public NotionDatabaseItemReaderBuilder<T> pageSize(int pageSize) {
		this.pageSize = pageSize;
		return this;
	}

	/**
	 * Sets the flag that determines whether to save the state of the reader for restarts.
	 * @param saveState the save state flag
	 * @return this builder
	 * @see NotionDatabaseItemReader#setSaveState(boolean)
	 */
	public NotionDatabaseItemReaderBuilder<T> saveState(boolean saveState) {
		this.saveState = saveState;
		return this;
	}

	/**
	 * The name used to calculate the key within the {@link ExecutionContext}. Required if
	 * {@link #saveState(boolean)} is set to true. </br>
	 * @param name the unique name of the component
	 * @return this builder
	 * @see NotionDatabaseItemReader#setName(String)
	 */
	public NotionDatabaseItemReaderBuilder<T> name(String name) {
		this.name = name;
		return this;
	}

	/**
	 * Sets the maximum number of items to read.
	 * @param maxItemCount the maximum item count
	 * @return this builder
	 * @see NotionDatabaseItemReader#setMaxItemCount(int)
	 */
	public NotionDatabaseItemReaderBuilder<T> maxItemCount(int maxItemCount) {
		this.maxItemCount = maxItemCount;
		return this;
	}

	/**
	 * Sets the index of the item to start reading from.
	 * @param currentItemCount the current item count
	 * @return this builder
	 * @see NotionDatabaseItemReader#setCurrentItemCount(int)
	 */
	public NotionDatabaseItemReaderBuilder<T> currentItemCount(int currentItemCount) {
		this.currentItemCount = currentItemCount;
		return this;
	}

	/**
	 * Builds the {@link NotionDatabaseItemReader}.
	 * @return the built reader
	 * @throws IllegalArgumentException if required fields are missing
	 */
	public NotionDatabaseItemReader<T> build() {
		if (this.saveState) {
			Assert.hasText(this.name, "A name is required when saveState is set to true.");
		}

		NotionDatabaseItemReader<T> reader = new NotionDatabaseItemReader<>(token, databaseId, propertyMapper);

		reader.setSaveState(saveState);
		if (baseUrl != null) {
			reader.setBaseUrl(baseUrl);
		}
		if (name != null) {
			reader.setName(name);
		}
		reader.setFilter(filter);
		reader.setSorts(sorts);
		reader.setPageSize(pageSize);
		reader.setMaxItemCount(maxItemCount);
		reader.setCurrentItemCount(currentItemCount);

		return reader;
	}

}
