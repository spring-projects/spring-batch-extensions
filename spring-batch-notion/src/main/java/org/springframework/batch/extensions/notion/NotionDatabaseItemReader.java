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
package org.springframework.batch.extensions.notion;

import org.jspecify.annotations.Nullable;
import org.springframework.batch.extensions.notion.PageProperty.RichTextProperty;
import org.springframework.batch.extensions.notion.PageProperty.TitleProperty;
import org.springframework.batch.extensions.notion.mapping.PropertyMapper;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.data.AbstractPaginatedDataItemReader;
import org.springframework.http.HttpHeaders;
import org.springframework.util.Assert;
import org.springframework.web.client.ApiVersionInserter;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Restartable {@link ItemReader} that reads entries from a Notion database via a paging
 * technique.
 * <p>
 * The query is executed using paged requests of a size specified in
 * {@link #setPageSize(int)}, which defaults to {@value #DEFAULT_PAGE_SIZE}. Additional
 * pages are requested as needed when the {@link #read()} method is called. On restart,
 * the reader will begin again at the same number item it left off at.
 * <p>
 * This implementation is thread-safe between calls to {@link #open(ExecutionContext)},
 * but remember to set <code>saveState</code> to <code>false</code> if used in a
 * multi-threaded environment (no restart available).
 *
 * @author Stefano Cordio
 * @param <T> Type of item to be read
 */
public class NotionDatabaseItemReader<T> extends AbstractPaginatedDataItemReader<T> {

	private static final int DEFAULT_PAGE_SIZE = 100;

	private static final String DEFAULT_BASE_URL = "https://api.notion.com/v1";

	private final String token;

	private final String databaseId;

	private final PropertyMapper<T> propertyMapper;

	private String baseUrl = DEFAULT_BASE_URL;

	private @Nullable Filter filter;

	private Sort[] sorts = new Sort[0];

	private @Nullable NotionDatabaseService service;

	private boolean hasMore;

	private @Nullable String nextCursor;

	/**
	 * Create a new {@link NotionDatabaseItemReader}.
	 * @param token the Notion integration token
	 * @param databaseId UUID of the database to read from
	 * @param propertyMapper the {@link PropertyMapper} responsible for mapping properties
	 * of a Notion item into a Java object
	 */
	public NotionDatabaseItemReader(String token, String databaseId, PropertyMapper<T> propertyMapper) {
		this.token = Objects.requireNonNull(token);
		this.databaseId = Objects.requireNonNull(databaseId);
		this.propertyMapper = Objects.requireNonNull(propertyMapper);
		this.pageSize = DEFAULT_PAGE_SIZE;
	}

	/**
	 * The base URL of the Notion API.
	 * <p>
	 * Defaults to {@value #DEFAULT_BASE_URL}.
	 * <p>
	 * A custom value can be provided for testing purposes (e.g., the URL of a WireMock
	 * server).
	 * @param baseUrl the base URL
	 */
	public void setBaseUrl(String baseUrl) {
		this.baseUrl = Objects.requireNonNull(baseUrl);
	}

	/**
	 * {@link Filter} condition to limit the returned items.
	 * <p>
	 * If no filter is provided, all the items in the database will be returned.
	 * @param filter the {@link Filter} conditions
	 * @see Filter#where()
	 * @see Filter#where(Filter)
	 */
	public void setFilter(Filter filter) {
		this.filter = filter;
	}

	/**
	 * {@link Sort} conditions to order the returned items.
	 * <p>
	 * Each condition is applied following the declaration order, i.e., earlier sorts take
	 * precedence over later ones.
	 * @param sorts the {@link Sort} conditions
	 * @see Sort#by(String)
	 * @see Sort#by(Sort.Timestamp)
	 */
	public void setSorts(Sort... sorts) {
		this.sorts = sorts;
	}

	/**
	 * The number of items to be read with each page.
	 * <p>
	 * Defaults to {@value #DEFAULT_PAGE_SIZE}.
	 * @param pageSize the number of items. Must be greater than 0 and less than or equal
	 * to 100.
	 */
	@Override
	public void setPageSize(int pageSize) {
		Assert.isTrue(pageSize <= 100, "pageSize must be less than or equal to 100");
		super.setPageSize(pageSize);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void doOpen() {
		RestClient restClient = RestClient.builder()
			.baseUrl(baseUrl)
			.apiVersionInserter(ApiVersionInserter.useHeader("Notion-Version"))
			.defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token)
			.build();

		RestClientAdapter adapter = RestClientAdapter.create(restClient);
		HttpServiceProxyFactory factory = HttpServiceProxyFactory.builderFor(adapter).build();
		service = factory.createClient(NotionDatabaseService.class);

		hasMore = true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected Iterator<T> doPageRead() {
		if (!hasMore) {
			return Collections.emptyIterator();
		}

		QueryRequest request = new QueryRequest(pageSize, nextCursor, filter, sorts);

		@SuppressWarnings("DataFlowIssue")
		QueryResult result = service.query(databaseId, request);

		hasMore = result.hasMore();
		nextCursor = result.nextCursor();

		return result.results()
			.stream()
			.map(NotionDatabaseItemReader::getProperties)
			.map(propertyMapper::map)
			.iterator();
	}

	private static Map<String, String> getProperties(Page page) {
		return page.properties()
			.entrySet()
			.stream()
			.collect(Collectors.toUnmodifiableMap(Entry::getKey, entry -> getPropertyValue(entry.getValue())));
	}

	private static String getPropertyValue(PageProperty property) {
		if (property instanceof RichTextProperty p) {
			return getPlainText(p.richText());
		}
		if (property instanceof TitleProperty p) {
			return getPlainText(p.title());
		}
		throw new IllegalArgumentException("Unsupported type: " + property.getClass());
	}

	private static String getPlainText(List<RichText> texts) {
		return texts.isEmpty() ? "" : texts.get(0).plainText();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void doClose() {
		hasMore = false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void jumpToItem(int itemIndex) throws Exception {
		for (int i = 0; i < itemIndex; i++) {
			read();
		}
	}

}
