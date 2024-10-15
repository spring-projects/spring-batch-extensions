/*
 * Copyright 2002-2024 the original author or authors.
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

import notion.api.v1.model.databases.query.filter.CompoundFilterElement;
import notion.api.v1.model.databases.query.filter.QueryTopLevelFilter;
import notion.api.v1.model.databases.query.filter.condition.CheckboxFilter;
import notion.api.v1.model.databases.query.filter.condition.MultiSelectFilter;
import notion.api.v1.model.databases.query.filter.condition.NumberFilter;
import notion.api.v1.model.databases.query.filter.condition.SelectFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Filtering conditions to limit the entries returned from a database query.
 * <p>
 * Filters operate on property values or entry timestamps, and can be combined.
 * <p>
 * A filter definition starts with {@link #where()}, entry point for a fluent API that
 * mimics the database filter option in the Notion UI.
 *
 * @author Stefano Cordio
 */
public abstract sealed class Filter {

	/**
	 * Entry point that starts the definition of a filter.
	 * @return a new {@link FilterConditionBuilder} instance
	 */
	public static FilterConditionBuilder<TopLevelFilter> where() {
		return new FilterConditionBuilder<>(PropertyFilter::new);
	}

	/**
	 * Entry point that starts the definition of a filter group.
	 * @param filter the filter representing the group
	 * @return a new {@link TopLevelFilter} instance that delegates to the given filter
	 */
	public static TopLevelFilter where(Filter filter) {
		return new DelegateFilter(filter);
	}

	private Filter() {
	}

	abstract QueryTopLevelFilter toQueryTopLevelFilter();

	abstract CompoundFilterElement toCompoundFilterElement();

	/**
	 * Base class for top level filters that support filters composition via the
	 * {@link TopLevelFilter#and} and {@link TopLevelFilter#or} methods.
	 *
	 * @see AndFilter
	 * @see OrFilter
	 */
	public static abstract sealed class TopLevelFilter extends Filter {

		private TopLevelFilter() {
		}

		/**
		 * Start the definition of a new filter that is composed with the current filter
		 * via a logical {@code and}.
		 * @return a {@link FilterConditionBuilder} instance for an {@link AndFilter}
		 */
		public FilterConditionBuilder<AndFilter> and() {
			return new FilterConditionBuilder<>(
					(property, customizer) -> new AndFilter(this, new PropertyFilter(property, customizer)));
		}

		/**
		 * Compose the current filter and the given filter via a logical {@code and}.
		 * @param filter the filter to compose with the current filter
		 * @return a new {@link AndFilter} instance
		 */
		public AndFilter and(Filter filter) {
			return new AndFilter(this, Objects.requireNonNull(filter));
		}

		/**
		 * Start the definition of a new filter that is composed with the current filter
		 * via a logical {@code or}.
		 * @return a {@link FilterConditionBuilder} instance for an {@link OrFilter}
		 */
		public FilterConditionBuilder<OrFilter> or() {
			return new FilterConditionBuilder<>(
					(property, customizer) -> new OrFilter(this, new PropertyFilter(property, customizer)));
		}

		/**
		 * Compose the current filter and the given filter via a logical {@code or}.
		 * @param filter the filter to compose with the current filter
		 * @return a new {@link OrFilter} instance
		 */
		public OrFilter or(Filter filter) {
			return new OrFilter(this, Objects.requireNonNull(filter));
		}

	}

	private static final class DelegateFilter extends TopLevelFilter {

		private final Filter delegate;

		private DelegateFilter(Filter delegate) {
			this.delegate = Objects.requireNonNull(delegate);
		}

		@Override
		QueryTopLevelFilter toQueryTopLevelFilter() {
			return delegate.toQueryTopLevelFilter();
		}

		@Override
		CompoundFilterElement toCompoundFilterElement() {
			return delegate.toCompoundFilterElement();
		}

	}

	private static final class PropertyFilter extends TopLevelFilter {

		private final String property;

		private final NotionPropertyFilterCustomizer customizer;

		private PropertyFilter(String property, NotionPropertyFilterCustomizer customizer) {
			this.property = property;
			this.customizer = customizer;
		}

		@Override
		QueryTopLevelFilter toQueryTopLevelFilter() {
			return toNotionPropertyFilter();
		}

		@Override
		CompoundFilterElement toCompoundFilterElement() {
			return toNotionPropertyFilter();
		}

		private notion.api.v1.model.databases.query.filter.PropertyFilter toNotionPropertyFilter() {
			var notionPropertyFilter = new notion.api.v1.model.databases.query.filter.PropertyFilter(property);
			customizer.accept(notionPropertyFilter);
			return notionPropertyFilter;
		}

	}

	@FunctionalInterface
	private interface NotionPropertyFilterFactory<T extends Filter>
			extends BiFunction<String, NotionPropertyFilterCustomizer, T> {

	}

	@FunctionalInterface
	private interface NotionPropertyFilterCustomizer
			extends Consumer<notion.api.v1.model.databases.query.filter.PropertyFilter> {

	}

	static abstract sealed class CompoundFilter extends Filter {

		final List<Filter> filters = new ArrayList<>();

		private final NotionCompoundFilterSetter setter;

		private CompoundFilter(NotionCompoundFilterSetter setter) {
			this.setter = setter;
		}

		@Override
		QueryTopLevelFilter toQueryTopLevelFilter() {
			return toNotionCompoundFilter();
		}

		@Override
		CompoundFilterElement toCompoundFilterElement() {
			return toNotionCompoundFilter();
		}

		private notion.api.v1.model.databases.query.filter.CompoundFilter toNotionCompoundFilter() {
			var notionCompoundFilter = new notion.api.v1.model.databases.query.filter.CompoundFilter();
			var notionCompoundFilterElements = filters.stream().map(Filter::toCompoundFilterElement).toList();
			setter.accept(notionCompoundFilter, notionCompoundFilterElements);
			return notionCompoundFilter;
		}

	}

	@FunctionalInterface
	private interface NotionCompoundFilterSetter
			extends BiConsumer<notion.api.v1.model.databases.query.filter.CompoundFilter, List<CompoundFilterElement>> {

	}

	/**
	 * Compound filter that supports filters composition via the {@link AndFilter#and}
	 * methods.
	 * <p>
	 * Returns entries that match <b>all</b> of the provided filters.
	 */
	public static final class AndFilter extends CompoundFilter {

		private AndFilter(Filter first, Filter second) {
			super(notion.api.v1.model.databases.query.filter.CompoundFilter::setAnd);
			filters.addAll(List.of(first, second));
		}

		/**
		 * Start the definition of a new filter that is composed with the current filter
		 * via a logical {@code and}.
		 * @return a {@link FilterConditionBuilder} instance for an {@link AndFilter}
		 */
		public FilterConditionBuilder<AndFilter> and() {
			return new FilterConditionBuilder<>((property, customizer) -> {
				filters.add(new PropertyFilter(property, customizer));
				return this;
			});
		}

		/**
		 * Compose the current filter and the given filter via a logical {@code and}.
		 * @param filter the filter to compose with the current filter
		 * @return a new {@link AndFilter} instance
		 */
		public AndFilter and(Filter filter) {
			filters.add(Objects.requireNonNull(filter));
			return this;
		}

	}

	/**
	 * Compound filter that supports filters composition via the {@link OrFilter#or}
	 * methods.
	 * <p>
	 * Returns entries that match <b>any</b> of the provided filters.
	 */
	public static final class OrFilter extends CompoundFilter {

		private OrFilter(Filter first, Filter second) {
			super(notion.api.v1.model.databases.query.filter.CompoundFilter::setOr);
			filters.addAll(List.of(first, second));
		}

		/**
		 * Start the definition of a new filter that is composed with the current filter
		 * via a logical {@code or}.
		 * @return a {@link FilterConditionBuilder} instance for an {@link OrFilter}
		 */
		public FilterConditionBuilder<OrFilter> or() {
			return new FilterConditionBuilder<>((property, customizer) -> {
				filters.add(new PropertyFilter(property, customizer));
				return this;
			});
		}

		/**
		 * Compose the current filter and the given filter via a logical {@code or}.
		 * @param filter the filter to compose with the current filter
		 * @return a new {@link OrFilter} instance
		 */
		public OrFilter or(Filter filter) {
			filters.add(Objects.requireNonNull(filter));
			return this;
		}

	}

	/**
	 * Builder for {@link Filter} conditions.
	 *
	 * @param <T> the type of the target {@code Filter}
	 */
	public static final class FilterConditionBuilder<T extends Filter> {

		private final NotionPropertyFilterFactory<T> factory;

		private FilterConditionBuilder(NotionPropertyFilterFactory<T> factory) {
			this.factory = factory;
		}

		/**
		 * Start the definition of the filter condition for a {@code checkbox} property.
		 * @param property The name of the property as it appears in the database, or the
		 * property ID
		 * @return a new {@link CheckboxCondition} instance
		 */
		public CheckboxCondition<T> checkbox(String property) {
			return new CheckboxCondition<>(property, factory);
		}

		/**
		 * Start the definition of the filter condition for a {@code multi-select}
		 * property.
		 * @param property The name of the property as it appears in the database, or the
		 * property ID
		 * @return a new {@link MultiSelectCondition} instance
		 */
		public MultiSelectCondition<T> multiSelect(String property) {
			return new MultiSelectCondition<>(property, factory);
		}

		/**
		 * Start the definition of the filter condition for a {@code number} property.
		 * @param property The name of the property as it appears in the database, or the
		 * property ID
		 * @return a new {@link NumberCondition} instance
		 */
		public NumberCondition<T> number(String property) {
			return new NumberCondition<>(property, factory);
		}

		/**
		 * Start the definition of the filter condition for a {@code select} property.
		 * @param property The name of the property as it appears in the database, or the
		 * property ID
		 * @return a new {@link SelectCondition} instance
		 */
		public SelectCondition<T> select(String property) {
			return new SelectCondition<>(property, factory);
		}

		static abstract sealed class Condition<T extends Filter> {

			private final String property;

			private final NotionPropertyFilterFactory<T> factory;

			private Condition(String property, NotionPropertyFilterFactory<T> factory) {
				this.property = property;
				this.factory = factory;
			}

			T toFilter(NotionPropertyFilterCustomizer customizer) {
				return factory.apply(property, customizer);
			}

		}

		/**
		 * Filter condition for a {@code checkbox} property.
		 *
		 * @param <T> the type of the target filter
		 */
		public static final class CheckboxCondition<T extends Filter> extends Condition<T> {

			private CheckboxCondition(String property, NotionPropertyFilterFactory<T> factory) {
				super(property, factory);
			}

			/**
			 * Return all database entries with an exact value match.
			 * @param value the value to match
			 * @return a filter with the newly defined condition
			 */
			public T isEqualTo(boolean value) {
				CheckboxFilter checkboxFilter = new CheckboxFilter();
				checkboxFilter.setEquals(value);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setCheckbox(checkboxFilter));
			}

			/**
			 * Return all database entries without an exact value match.
			 * @param value the value to differ with
			 * @return a filter with the newly defined condition
			 */
			public T isNotEqualTo(boolean value) {
				CheckboxFilter checkboxFilter = new CheckboxFilter();
				checkboxFilter.setDoesNotEqual(value);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setCheckbox(checkboxFilter));
			}

		}

		/**
		 * Filter condition for a {@code multi-select} property.
		 *
		 * @param <T> the type of the target filter
		 */
		public static final class MultiSelectCondition<T extends Filter> extends Condition<T> {

			private MultiSelectCondition(String property, NotionPropertyFilterFactory<T> factory) {
				super(property, factory);
			}

			/**
			 * Return database entries where the provided value is part of the property
			 * values.
			 * @param value the value to compare the property values against
			 * @return a filter with the newly defined condition
			 */
			public T contains(String value) {
				MultiSelectFilter multiSelectFilter = new MultiSelectFilter();
				multiSelectFilter.setContains(value);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setMultiSelect(multiSelectFilter));
			}

			/**
			 * Return database entries where the provided value is not contained in the
			 * property values.
			 * @param value the value to compare the property values against
			 * @return a filter with the newly defined condition
			 */
			public T doesNotContain(String value) {
				MultiSelectFilter multiSelectFilter = new MultiSelectFilter();
				multiSelectFilter.setDoesNotContain(value);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setMultiSelect(multiSelectFilter));
			}

			/**
			 * Return database entries where the property value does not contain any data.
			 * @return a filter with the newly defined condition
			 */
			public T isEmpty() {
				MultiSelectFilter multiSelectFilter = new MultiSelectFilter();
				multiSelectFilter.setEmpty(true);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setMultiSelect(multiSelectFilter));
			}

			/**
			 * Return database entries where the property value contains data.
			 * @return a filter with the newly defined condition
			 */
			public T isNotEmpty() {
				MultiSelectFilter multiSelectFilter = new MultiSelectFilter();
				multiSelectFilter.setNotEmpty(true);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setMultiSelect(multiSelectFilter));
			}

		}

		/**
		 * Filter condition for a {@code number} property.
		 *
		 * @param <T> the type of the target filter
		 */
		public static final class NumberCondition<T extends Filter> extends Condition<T> {

			private NumberCondition(String property, NotionPropertyFilterFactory<T> factory) {
				super(property, factory);
			}

			/**
			 * Return database entries where the property value is the same as the
			 * provided one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isEqualTo(int value) {
				NumberFilter numberFilter = new NumberFilter();
				numberFilter.setEquals(value);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setNumber(numberFilter));
			}

			/**
			 * Return database entries where the property value differs from the provided
			 * one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isNotEqualTo(int value) {
				NumberFilter numberFilter = new NumberFilter();
				numberFilter.setDoesNotEqual(value);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setNumber(numberFilter));
			}

			/**
			 * Return database entries where the property value exceeds the provided one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isGreaterThan(int value) {
				NumberFilter numberFilter = new NumberFilter();
				numberFilter.setGreaterThan(value);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setNumber(numberFilter));
			}

			/**
			 * Return database entries where the property value is equal to or exceeds the
			 * provided one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isGreaterThanOrEqualTo(int value) {
				NumberFilter numberFilter = new NumberFilter();
				numberFilter.setGreaterThanOrEqualTo(value);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setNumber(numberFilter));
			}

			/**
			 * Return database entries where the property value is less than the provided
			 * one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isLessThan(int value) {
				NumberFilter numberFilter = new NumberFilter();
				numberFilter.setLessThan(value);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setNumber(numberFilter));
			}

			/**
			 * Return database entries where the property value is equal to or is less
			 * than the provided one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isLessThanOrEqualTo(int value) {
				NumberFilter numberFilter = new NumberFilter();
				numberFilter.setLessThanOrEqualTo(value);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setNumber(numberFilter));
			}

			/**
			 * Return database entries where the property value does not contain any data.
			 * @return a filter with the newly defined condition
			 */
			public T isEmpty() {
				NumberFilter numberFilter = new NumberFilter();
				numberFilter.setEmpty(true);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setNumber(numberFilter));
			}

			/**
			 * Return database entries where the property value contains data.
			 * @return a filter with the newly defined condition
			 */
			public T isNotEmpty() {
				NumberFilter numberFilter = new NumberFilter();
				numberFilter.setNotEmpty(true);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setNumber(numberFilter));
			}

		}

		/**
		 * Filter condition for a {@code select} property.
		 *
		 * @param <T> the type of the target filter
		 */
		public static final class SelectCondition<T extends Filter> extends Condition<T> {

			private SelectCondition(String property, NotionPropertyFilterFactory<T> factory) {
				super(property, factory);
			}

			/**
			 * Return database entries where the property value matches the provided one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isEqualTo(String value) {
				SelectFilter selectFilter = new SelectFilter();
				selectFilter.setEquals(value);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setSelect(selectFilter));
			}

			/**
			 * Return database entries where the property value does not match the
			 * provided one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isNotEqualTo(String value) {
				SelectFilter selectFilter = new SelectFilter();
				selectFilter.setDoesNotEqual(value);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setSelect(selectFilter));
			}

			/**
			 * Return database entries where the property value does not contain any data.
			 * @return a filter with the newly defined condition
			 */
			public T isEmpty() {
				SelectFilter selectFilter = new SelectFilter();
				selectFilter.setEmpty(true);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setSelect(selectFilter));
			}

			/**
			 * Return database entries where the property value contains data.
			 * @return a filter with the newly defined condition
			 */
			public T isNotEmpty() {
				SelectFilter selectFilter = new SelectFilter();
				selectFilter.setNotEmpty(true);
				return toFilter(notionPropertyFilter -> notionPropertyFilter.setSelect(selectFilter));
			}

		}

	}

}
