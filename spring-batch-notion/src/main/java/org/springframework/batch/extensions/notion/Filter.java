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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import org.jspecify.annotations.Nullable;
import org.springframework.batch.extensions.notion.Filter.FilterConditionBuilder.Condition;
import tools.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy;
import tools.jackson.databind.annotation.JsonNaming;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.BiFunction;

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
@JsonTypeName("filter")
@JsonTypeInfo(use = Id.NAME, include = As.WRAPPER_OBJECT)
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
					(property, condition) -> new AndFilter(this, new PropertyFilter(property, condition)));
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
					(property, condition) -> new OrFilter(this, new PropertyFilter(property, condition)));
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

		@JsonValue
		private final Filter delegate;

		private DelegateFilter(Filter delegate) {
			this.delegate = delegate;
		}

	}

	private static final class PropertyFilter extends TopLevelFilter {

		@SuppressWarnings("unused")
		@JsonProperty
		private final String property;

		@SuppressWarnings("unused")
		@JsonAnyGetter
		private final Map<String, Condition<?>> condition;

		private PropertyFilter(String property, Entry<String, Condition<?>> condition) {
			this.property = property;
			this.condition = Map.ofEntries(condition);
		}

		@Override
		public String toString() {
			return new StringJoiner(", ", PropertyFilter.class.getSimpleName() + "[", "]")
				.add("property='" + property + "'")
				.add(condition.toString())
				.toString();
		}

	}

	@FunctionalInterface
	private interface PropertyFilterFactory<T extends Filter>
			extends BiFunction<String, Entry<String, Condition<?>>, T> {

	}

	/**
	 * Compound filter that supports filters composition via the {@link AndFilter#and}
	 * methods.
	 * <p>
	 * Returns entries that match <b>all</b> of the provided filters.
	 */
	public static final class AndFilter extends Filter {

		@JsonProperty
		@JsonTypeInfo(use = Id.DEDUCTION)
		private final List<Filter> and = new ArrayList<>();

		private AndFilter(Filter first, Filter second) {
			and.addAll(List.of(first, second));
		}

		/**
		 * Start the definition of a new filter that is composed with the current filter
		 * via a logical {@code and}.
		 * @return a {@link FilterConditionBuilder} instance for an {@link AndFilter}
		 */
		public FilterConditionBuilder<AndFilter> and() {
			return new FilterConditionBuilder<>((property, condition) -> {
				and.add(new PropertyFilter(property, condition));
				return this;
			});
		}

		/**
		 * Compose the current filter and the given filter via a logical {@code and}.
		 * @param filter the filter to compose with the current filter
		 * @return a new {@link AndFilter} instance
		 */
		public AndFilter and(Filter filter) {
			and.add(Objects.requireNonNull(filter));
			return this;
		}

	}

	/**
	 * Compound filter that supports filters composition via the {@link OrFilter#or}
	 * methods.
	 * <p>
	 * Returns entries that match <b>any</b> of the provided filters.
	 */
	public static final class OrFilter extends Filter {

		@JsonProperty
		@JsonTypeInfo(use = Id.DEDUCTION)
		private final List<Filter> or = new ArrayList<>();

		private OrFilter(Filter first, Filter second) {
			or.addAll(List.of(first, second));
		}

		/**
		 * Start the definition of a new filter that is composed with the current filter
		 * via a logical {@code or}.
		 * @return a {@link FilterConditionBuilder} instance for an {@link OrFilter}
		 */
		public FilterConditionBuilder<OrFilter> or() {
			return new FilterConditionBuilder<>((property, condition) -> {
				or.add(new PropertyFilter(property, condition));
				return this;
			});
		}

		/**
		 * Compose the current filter and the given filter via a logical {@code or}.
		 * @param filter the filter to compose with the current filter
		 * @return a new {@link OrFilter} instance
		 */
		public OrFilter or(Filter filter) {
			or.add(Objects.requireNonNull(filter));
			return this;
		}

	}

	/**
	 * Builder for {@link Filter} conditions.
	 *
	 * @param <T> the type of the target {@code Filter}
	 */
	public static final class FilterConditionBuilder<T extends Filter> {

		private final PropertyFilterFactory<T> factory;

		private FilterConditionBuilder(PropertyFilterFactory<T> factory) {
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
		 * Start the definition of the filter condition for a {@code files} property.
		 * @param property The name of the property as it appears in the database, or the
		 * property ID
		 * @return a new {@link FilesCondition} instance
		 */
		public FilesCondition<T> files(String property) {
			return new FilesCondition<>(property, factory);
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

		/**
		 * Start the definition of the filter condition for a {@code status} property.
		 * @param property The name of the property as it appears in the database, or the
		 * property ID
		 * @return a new {@link StatusCondition} instance
		 */
		public StatusCondition<T> status(String property) {
			return new StatusCondition<>(property, factory);
		}

		@JsonNaming(SnakeCaseStrategy.class)
		@JsonInclude(Include.NON_EMPTY)
		static abstract sealed class Condition<T extends Filter> {

			private final String name;

			private final String property;

			private final PropertyFilterFactory<T> factory;

			private Condition(String name, String property, PropertyFilterFactory<T> factory) {
				this.name = name;
				this.property = property;
				this.factory = factory;
			}

			T toFilter() {
				return factory.apply(property, new SimpleEntry<>(name, this));
			}

		}

		/**
		 * Filter condition for a {@code checkbox} property.
		 *
		 * @param <T> the type of the target filter
		 */
		public static final class CheckboxCondition<T extends Filter> extends Condition<T> {

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Boolean equals;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Boolean doesNotEqual;

			private CheckboxCondition(String property, PropertyFilterFactory<T> factory) {
				super("checkbox", property, factory);
			}

			/**
			 * Return all database entries with an exact value match.
			 * @param value the value to match
			 * @return a filter with the newly defined condition
			 */
			public T isEqualTo(boolean value) {
				this.equals = value;
				return toFilter();
			}

			/**
			 * Return all database entries without an exact value match.
			 * @param value the value to differ with
			 * @return a filter with the newly defined condition
			 */
			public T isNotEqualTo(boolean value) {
				this.doesNotEqual = value;
				return toFilter();
			}

		}

		/**
		 * Filter condition for a {@code files} property.
		 *
		 * @param <T> the type of the target filter
		 */
		public static final class FilesCondition<T extends Filter> extends Condition<T> {

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Boolean isEmpty;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Boolean isNotEmpty;

			private FilesCondition(String property, PropertyFilterFactory<T> factory) {
				super("files", property, factory);
			}

			/**
			 * Return database entries where the property value does not contain any data.
			 * @return a filter with the newly defined condition
			 */
			public T isEmpty() {
				this.isEmpty = true;
				return toFilter();
			}

			/**
			 * Return database entries where the property value contains data.
			 * @return a filter with the newly defined condition
			 */
			public T isNotEmpty() {
				this.isNotEmpty = true;
				return toFilter();
			}

		}

		/**
		 * Filter condition for a {@code multi-select} property.
		 *
		 * @param <T> the type of the target filter
		 */
		public static final class MultiSelectCondition<T extends Filter> extends Condition<T> {

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable String contains;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable String doesNotContain;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Boolean isEmpty;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Boolean isNotEmpty;

			private MultiSelectCondition(String property, PropertyFilterFactory<T> factory) {
				super("multi_select", property, factory);
			}

			/**
			 * Return database entries where the provided value is part of the property
			 * values.
			 * @param value the value to compare the property values against
			 * @return a filter with the newly defined condition
			 */
			public T contains(String value) {
				this.contains = value;
				return toFilter();
			}

			/**
			 * Return database entries where the provided value is not contained in the
			 * property values.
			 * @param value the value to compare the property values against
			 * @return a filter with the newly defined condition
			 */
			public T doesNotContain(String value) {
				this.doesNotContain = value;
				return toFilter();
			}

			/**
			 * Return database entries where the property value does not contain any data.
			 * @return a filter with the newly defined condition
			 */
			public T isEmpty() {
				this.isEmpty = true;
				return toFilter();
			}

			/**
			 * Return database entries where the property value contains data.
			 * @return a filter with the newly defined condition
			 */
			public T isNotEmpty() {
				this.isNotEmpty = true;
				return toFilter();
			}

		}

		/**
		 * Filter condition for a {@code number} property.
		 *
		 * @param <T> the type of the target filter
		 */
		public static final class NumberCondition<T extends Filter> extends Condition<T> {

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Integer equals;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Integer doesNotEqual;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Integer greaterThan;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Integer greaterThanOrEqualTo;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Integer lessThan;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Integer lessThanOrEqualTo;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Boolean isEmpty;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Boolean isNotEmpty;

			private NumberCondition(String property, PropertyFilterFactory<T> factory) {
				super("number", property, factory);
			}

			/**
			 * Return database entries where the property value is the same as the
			 * provided one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isEqualTo(int value) {
				this.equals = value;
				return toFilter();
			}

			/**
			 * Return database entries where the property value differs from the provided
			 * one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isNotEqualTo(int value) {
				this.doesNotEqual = value;
				return toFilter();
			}

			/**
			 * Return database entries where the property value exceeds the provided one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isGreaterThan(int value) {
				this.greaterThan = value;
				return toFilter();
			}

			/**
			 * Return database entries where the property value is equal to or exceeds the
			 * provided one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isGreaterThanOrEqualTo(int value) {
				this.greaterThanOrEqualTo = value;
				return toFilter();
			}

			/**
			 * Return database entries where the property value is less than the provided
			 * one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isLessThan(int value) {
				this.lessThan = value;
				return toFilter();
			}

			/**
			 * Return database entries where the property value is equal to or is less
			 * than the provided one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isLessThanOrEqualTo(int value) {
				this.lessThanOrEqualTo = value;
				return toFilter();
			}

			/**
			 * Return database entries where the property value does not contain any data.
			 * @return a filter with the newly defined condition
			 */
			public T isEmpty() {
				this.isEmpty = true;
				return toFilter();
			}

			/**
			 * Return database entries where the property value contains data.
			 * @return a filter with the newly defined condition
			 */
			public T isNotEmpty() {
				this.isNotEmpty = true;
				return toFilter();
			}

		}

		/**
		 * Filter condition for a {@code select} property.
		 *
		 * @param <T> the type of the target filter
		 */
		public static final class SelectCondition<T extends Filter> extends Condition<T> {

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable String equals;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable String doesNotEqual;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Boolean isEmpty;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Boolean isNotEmpty;

			private SelectCondition(String property, PropertyFilterFactory<T> factory) {
				super("select", property, factory);
			}

			/**
			 * Return database entries where the property value matches the provided one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isEqualTo(String value) {
				this.equals = value;
				return toFilter();
			}

			/**
			 * Return database entries where the property value does not match the
			 * provided one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isNotEqualTo(String value) {
				this.doesNotEqual = value;
				return toFilter();
			}

			/**
			 * Return database entries where the property value does not contain any data.
			 * @return a filter with the newly defined condition
			 */
			public T isEmpty() {
				this.isEmpty = true;
				return toFilter();
			}

			/**
			 * Return database entries where the property value contains data.
			 * @return a filter with the newly defined condition
			 */
			public T isNotEmpty() {
				this.isNotEmpty = true;
				return toFilter();
			}

		}

		/**
		 * Filter condition for a {@code status} property.
		 *
		 * @param <T> the type of the target filter
		 */
		public static final class StatusCondition<T extends Filter> extends Condition<T> {

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable String equals;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable String doesNotEqual;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Boolean isEmpty;

			@SuppressWarnings("unused")
			@JsonProperty
			private @Nullable Boolean isNotEmpty;

			private StatusCondition(String property, PropertyFilterFactory<T> factory) {
				super("status", property, factory);
			}

			/**
			 * Return database entries where the property value matches the provided one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isEqualTo(String value) {
				this.equals = value;
				return toFilter();
			}

			/**
			 * Return database entries where the property value does not match the
			 * provided one.
			 * @param value the value to compare the property value against
			 * @return a filter with the newly defined condition
			 */
			public T isNotEqualTo(String value) {
				this.doesNotEqual = value;
				return toFilter();
			}

			/**
			 * Return database entries where the property value does not contain any data.
			 * @return a filter with the newly defined condition
			 */
			public T isEmpty() {
				this.isEmpty = true;
				return toFilter();
			}

			/**
			 * Return database entries where the property value contains data.
			 * @return a filter with the newly defined condition
			 */
			public T isNotEmpty() {
				this.isNotEmpty = true;
				return toFilter();
			}

		}

	}

}
