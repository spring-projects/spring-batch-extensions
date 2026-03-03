/*
 * Copyright 2024-2026 the original author or authors.
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
package org.springframework.batch.extensions.notion.builder;

import org.jspecify.annotations.Nullable;
import org.springframework.batch.extensions.notion.Filter;
import org.springframework.batch.extensions.notion.NotionDatabaseItemReader;
import org.springframework.batch.extensions.notion.Sort;
import org.springframework.batch.extensions.notion.mapping.PropertyMapper;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.util.Assert;

/**
 * A builder for the {@link NotionDatabaseItemReader}.
 *
 * @author Jaeung Ha
 * @param <T> Type of item to be read
 * @see NotionDatabaseItemReader
 * @since 0.2.0
 */
public class NotionDatabaseItemReaderBuilder<T> {

	private @Nullable String token;

	private @Nullable String databaseId;

	private @Nullable PropertyMapper<T> propertyMapper;

	private @Nullable String baseUrl;

	private @Nullable Filter filter;

	private @Nullable String name;

	private Sort[] sorts = new Sort[0];

	private int pageSize = NotionDatabaseItemReader.DEFAULT_PAGE_SIZE;

	private boolean saveState = true;

	private int maxItemCount = Integer.MAX_VALUE;

	private int currentItemCount = 0;

	/**
	 * Create a new {@link NotionDatabaseItemReaderBuilder}.
	 */
	public NotionDatabaseItemReaderBuilder() {
	}

	/**
	 * The Notion integration token.
	 * @param token the token
	 * @return the current instance of the builder
	 * @see NotionDatabaseItemReader#NotionDatabaseItemReader(String, String,
	 * PropertyMapper)
	 */
	public NotionDatabaseItemReaderBuilder<T> token(String token) {
		this.token = token;
		return this;
	}

	/**
	 * The UUID of the database to read from.
	 * @param databaseId the database UUID
	 * @return the current instance of the builder
	 * @see NotionDatabaseItemReader#NotionDatabaseItemReader(String, String,
	 * PropertyMapper)
	 */
	public NotionDatabaseItemReaderBuilder<T> databaseId(String databaseId) {
		this.databaseId = databaseId;
		return this;
	}

	/**
	 * The {@link PropertyMapper} responsible for mapping properties of a Notion item into
	 * a Java object.
	 * @param propertyMapper the property mapper
	 * @return the current instance of the builder
	 * @see NotionDatabaseItemReader#NotionDatabaseItemReader(String, String,
	 * PropertyMapper)
	 */
	public NotionDatabaseItemReaderBuilder<T> propertyMapper(PropertyMapper<T> propertyMapper) {
		this.propertyMapper = propertyMapper;
		return this;
	}

	/**
	 * The base URL of the Notion API.
	 * <p>
	 * Defaults to {@value NotionDatabaseItemReader#DEFAULT_BASE_URL}.
	 * <p>
	 * A custom value can be provided for testing purposes (e.g., the URL of a WireMock
	 * server).
	 * @param baseUrl the base URL
	 * @return the current instance of the builder
	 * @see NotionDatabaseItemReader#setBaseUrl(String)
	 */
	public NotionDatabaseItemReaderBuilder<T> baseUrl(String baseUrl) {
		this.baseUrl = baseUrl;
		return this;
	}

	/**
	 * {@link Filter} condition to limit the returned items.
	 * <p>
	 * If no filter is provided, all the items in the database will be returned.
	 * @param filter the filter
	 * @return the current instance of the builder
	 * @see NotionDatabaseItemReader#setFilter(Filter)
	 */
	public NotionDatabaseItemReaderBuilder<T> filter(Filter filter) {
		this.filter = filter;
		return this;
	}

	/**
	 * {@link Sort} conditions to order the returned items.
	 * <p>
	 * Each condition is applied following the declaration order, i.e., earlier sorts take
	 * precedence over later ones.
	 * @param sorts the {@link Sort} conditions
	 * @return the current instance of the builder
	 * @see NotionDatabaseItemReader#setSorts(Sort...)
	 */
	public NotionDatabaseItemReaderBuilder<T> sorts(Sort... sorts) {
		this.sorts = sorts;
		return this;
	}

	/**
	 * The number of items to be read with each page.
	 * <p>
	 * Defaults to {@value NotionDatabaseItemReader#DEFAULT_PAGE_SIZE}.
	 * @param pageSize the page size
	 * @return the current instance of the builder
	 * @see NotionDatabaseItemReader#setPageSize(int)
	 */
	public NotionDatabaseItemReaderBuilder<T> pageSize(int pageSize) {
		this.pageSize = pageSize;
		return this;
	}

	/**
	 * Sets the flag that determines whether to save the state of the reader for restarts.
	 * @param saveState the save state flag
	 * @return the current instance of the builder
	 * @see NotionDatabaseItemReader#setSaveState(boolean)
	 */
	public NotionDatabaseItemReaderBuilder<T> saveState(boolean saveState) {
		this.saveState = saveState;
		return this;
	}

	/**
	 * The name of the component which will be used as a stem for keys in the
	 * {@link ExecutionContext}.
	 * @param name the name for the component
	 * @return the current instance of the builder
	 * @see NotionDatabaseItemReader#setName(String)
	 */
	public NotionDatabaseItemReaderBuilder<T> name(String name) {
		this.name = name;
		return this;
	}

	/**
	 * The maximum index of the items to be read.
	 * @param maxItemCount the maximum item count
	 * @return the current instance of the builder
	 * @see NotionDatabaseItemReader#setMaxItemCount(int)
	 */
	public NotionDatabaseItemReaderBuilder<T> maxItemCount(int maxItemCount) {
		this.maxItemCount = maxItemCount;
		return this;
	}

	/**
	 * The index of the item to start reading from.
	 * @param currentItemCount the current item count
	 * @return the current instance of the builder
	 * @see NotionDatabaseItemReader#setCurrentItemCount(int)
	 */
	public NotionDatabaseItemReaderBuilder<T> currentItemCount(int currentItemCount) {
		this.currentItemCount = currentItemCount;
		return this;
	}

	/**
	 * Builds the {@link NotionDatabaseItemReader}.
	 * @return the built reader
	 */
	public NotionDatabaseItemReader<T> build() {
		if (this.saveState && this.name != null) {
			Assert.hasText(this.name, "A name is required when saveState is set to true.");
		}

		if (token == null || databaseId == null || propertyMapper == null) {
			throw new IllegalArgumentException("token, databaseId, and propertyMapper must not be null");
		}
		NotionDatabaseItemReader<T> reader = new NotionDatabaseItemReader<>(token, databaseId, propertyMapper);

		reader.setSaveState(saveState);
		if (baseUrl != null) {
			reader.setBaseUrl(baseUrl);
		}
		if (name != null) {
			reader.setName(name);
		}
		if (filter != null) {
			reader.setFilter(filter);
		}
		reader.setSorts(sorts);
		reader.setPageSize(pageSize);
		reader.setMaxItemCount(maxItemCount);
		reader.setCurrentItemCount(currentItemCount);

		return reader;
	}

}
