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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;
import static org.springframework.batch.extensions.notion.Filter.where;

/**
 * @author Stefano Cordio
 */
class FilterTests {

	// Test cases from https://developers.notion.com/reference/post-database-query-filter

	private final JsonMapper jsonMapper = new JsonMapper();

	@ParameterizedTest
	@FieldSource({ "PROPERTY_FILTERS", "COMPOUND_FILTERS", "NESTED_FILTERS" })
	void toJson(Filter underTest, String expected) throws Exception {
		// WHEN
		String result = jsonMapper.writeValueAsString(underTest);
		// THEN
		assertEquals(expected, result, true);
	}

	static final List<Arguments> CHECKBOX_FILTERS = Stream.of(true, false)
		.flatMap(value -> Stream.of( //
				arguments(where().checkbox("Task completed").isEqualTo(value), """
						{
						  "filter": {
						    "property": "Task completed",
						    "checkbox": {
						      "equals": %s
						    }
						  }
						}
						""".formatted(value)), //
				arguments(where().checkbox("Task completed").isNotEqualTo(value), """
						{
						  "filter": {
						    "property": "Task completed",
						    "checkbox": {
						      "does_not_equal": %s
						    }
						  }
						}
						""".formatted(value))))
		.toList();

	static final List<Arguments> FILES_FILTERS = List.of( //
			arguments(where().files("Blueprint").isEmpty(), """
					{
					  "filter": {
					    "property": "Blueprint",
					    "files": {
					      "is_empty": true
					    }
					  }
					}
					"""), //
			arguments(where().files("Blueprint").isNotEmpty(), """
					{
					  "filter": {
					    "property": "Blueprint",
					    "files": {
					      "is_not_empty": true
					    }
					  }
					}
					"""));

	static final List<Arguments> MULTI_SELECT_FILTERS = List.of( //
			arguments(where().multiSelect("Programming language").contains("TypeScript"), """
					{
					  "filter": {
					    "property": "Programming language",
					    "multi_select": {
					      "contains": "TypeScript"
					    }
					  }
					}
					"""), //
			arguments(where().multiSelect("Programming language").doesNotContain("TypeScript"), """
					{
					  "filter": {
					    "property": "Programming language",
					    "multi_select": {
					      "does_not_contain": "TypeScript"
					    }
					  }
					}
					"""), //
			arguments(where().multiSelect("Programming language").isEmpty(), """
					{
					  "filter": {
					    "property": "Programming language",
					    "multi_select": {
					      "is_empty": true
					    }
					  }
					}
					"""), //
			arguments(where().multiSelect("Programming language").isNotEmpty(), """
					{
					  "filter": {
					    "property": "Programming language",
					    "multi_select": {
					      "is_not_empty": true
					    }
					  }
					}
					"""));

	static final List<Arguments> NUMBER_FILTERS = List.of( //
			arguments(where().number("Estimated working days").isEqualTo(42), """
					{
					  "filter": {
					    "property": "Estimated working days",
					    "number": {
					      "equals": 42
					    }
					  }
					}
					"""), //
			arguments(where().number("Estimated working days").isNotEqualTo(42), """
					{
					  "filter": {
					    "property": "Estimated working days",
					    "number": {
					      "does_not_equal": 42
					    }
					  }
					}
					"""), //
			arguments(where().number("Estimated working days").isGreaterThan(42), """
					{
					  "filter": {
					    "property": "Estimated working days",
					    "number": {
					      "greater_than": 42
					    }
					  }
					}
					"""), //
			arguments(where().number("Estimated working days").isGreaterThanOrEqualTo(42), """
					{
					  "filter": {
					    "property": "Estimated working days",
					    "number": {
					      "greater_than_or_equal_to": 42
					    }
					  }
					}
					"""), //
			arguments(where().number("Estimated working days").isLessThan(42), """
					{
					  "filter": {
					    "property": "Estimated working days",
					    "number": {
					      "less_than": 42
					    }
					  }
					}
					"""), //
			arguments(where().number("Estimated working days").isLessThanOrEqualTo(42), """
					{
					  "filter": {
					    "property": "Estimated working days",
					    "number": {
					      "less_than_or_equal_to": 42
					    }
					  }
					}
					"""), //
			arguments(where().number("Estimated working days").isEmpty(), """
					{
					  "filter": {
					    "property": "Estimated working days",
					    "number": {
					      "is_empty": true
					    }
					  }
					}
					"""), //
			arguments(where().number("Estimated working days").isNotEmpty(), """
					{
					  "filter": {
					    "property": "Estimated working days",
					    "number": {
					      "is_not_empty": true
					    }
					  }
					}
					"""));

	static final List<Arguments> SELECT_FILTERS = List.of( //
			arguments(where().select("Backend framework").isEqualTo("Spring"), """
					{
					  "filter": {
					    "property": "Backend framework",
					    "select": {
					      "equals": "Spring"
					    }
					  }
					}
					"""), //
			arguments(where().select("Backend framework").isNotEqualTo("Spring"), """
					{
					  "filter": {
					    "property": "Backend framework",
					    "select": {
					      "does_not_equal": "Spring"
					    }
					  }
					}
					"""), //
			arguments(where().select("Backend framework").isEmpty(), """
					{
					  "filter": {
					    "property": "Backend framework",
					    "select": {
					      "is_empty": true
					    }
					  }
					}
					"""), //
			arguments(where().select("Backend framework").isNotEmpty(), """
					{
					  "filter": {
					    "property": "Backend framework",
					    "select": {
					      "is_not_empty": true
					    }
					  }
					}
					"""));

	static final List<Arguments> STATUS_FILTERS = List.of( //
			arguments(where().status("Project status").isEqualTo("Not started"), """
					{
					  "filter": {
					    "property": "Project status",
					    "status": {
					      "equals": "Not started"
					    }
					  }
					}
					"""), //
			arguments(where().status("Project status").isNotEqualTo("Not started"), """
					{
					  "filter": {
					    "property": "Project status",
					    "status": {
					      "does_not_equal": "Not started"
					    }
					  }
					}
					"""), //
			arguments(where().status("Project status").isEmpty(), """
					{
					  "filter": {
					    "property": "Project status",
					    "status": {
					      "is_empty": true
					    }
					  }
					}
					"""), //
			arguments(where().status("Project status").isNotEmpty(), """
					{
					  "filter": {
					    "property": "Project status",
					    "status": {
					      "is_not_empty": true
					    }
					  }
					}
					"""));

	static final List<Arguments> PROPERTY_FILTERS = Stream.of( //
			CHECKBOX_FILTERS, //
			FILES_FILTERS, //
			MULTI_SELECT_FILTERS, //
			NUMBER_FILTERS, //
			SELECT_FILTERS, //
			STATUS_FILTERS) //
		.flatMap(List::stream)
		.toList();

	static final List<Arguments> AND_FILTERS = List.of( //
			arguments(where().checkbox("Complete").isEqualTo(true).and().number("Days").isGreaterThan(10), """
					{
					  "filter": {
					    "and": [
					      {
					        "property": "Complete",
					        "checkbox": {
					          "equals": true
					        }
					      },
					      {
					        "property": "Days",
					        "number": {
					          "greater_than": 10
					        }
					      }
					    ]
					  }
					}
					"""),
			arguments(where().checkbox("Complete").isEqualTo(true).and(where().number("Days").isGreaterThan(10)), """
					{
					  "filter": {
					    "and": [
					      {
					        "property": "Complete",
					        "checkbox": {
					          "equals": true
					        }
					      },
					      {
					        "property": "Days",
					        "number": {
					          "greater_than": 10
					        }
					      }
					    ]
					  }
					}
					"""),
			arguments( // @formatter:off
					where().checkbox("Complete").isEqualTo(true)
						.and(where().number("Days").isGreaterThan(10))
						.and().checkbox("Archived").isNotEqualTo(false)
						.and(where().select("Language").isEmpty()),
					// @formatter:on
					"""
							{
							  "filter": {
							    "and": [
							      {
							        "property": "Complete",
							        "checkbox": {
							          "equals": true
							        }
							      },
							      {
							        "property": "Days",
							        "number": {
							          "greater_than": 10
							        }
							      },
							      {
							        "property": "Archived",
							        "checkbox": {
							          "does_not_equal": false
							        }
							      },
							      {
							        "property": "Language",
							        "select": {
							          "is_empty": true
							        }
							      }
							    ]
							  }
							}
							"""));

	static final List<Arguments> OR_FILTERS = List.of( //
			arguments(where().checkbox("Complete").isEqualTo(true).or().number("Days").isGreaterThan(10), """
					{
					  "filter": {
					    "or": [
					      {
					        "property": "Complete",
					        "checkbox": {
					          "equals": true
					        }
					      },
					      {
					        "property": "Days",
					        "number": {
					          "greater_than": 10
					        }
					      }
					    ]
					  }
					}
					"""),
			arguments(where().checkbox("Complete").isEqualTo(true).or(where().number("Days").isGreaterThan(10)), """
					{
					  "filter": {
					    "or": [
					      {
					        "property": "Complete",
					        "checkbox": {
					          "equals": true
					        }
					      },
					      {
					        "property": "Days",
					        "number": {
					          "greater_than": 10
					        }
					      }
					    ]
					  }
					}
					"""),
			arguments( // @formatter:off
					where().checkbox("Complete").isEqualTo(true)
						.or(where().number("Days").isGreaterThan(10))
						.or().checkbox("Archived").isNotEqualTo(false)
						.or(where().select("Language").isEmpty()),
					// @formatter:on
					"""
							{
							  "filter": {
							    "or": [
							      {
							        "property": "Complete",
							        "checkbox": {
							          "equals": true
							        }
							      },
							      {
							        "property": "Days",
							        "number": {
							          "greater_than": 10
							        }
							      },
							      {
							        "property": "Archived",
							        "checkbox": {
							          "does_not_equal": false
							        }
							      },
							      {
							        "property": "Language",
							        "select": {
							          "is_empty": true
							        }
							      }
							    ]
							  }
							}
							"""));

	static final List<Arguments> COMPOUND_FILTERS = Stream.of(AND_FILTERS, OR_FILTERS).flatMap(List::stream).toList();

	static final List<Arguments> NESTED_FILTERS = List.of( //
			arguments(where(where().checkbox("Task completed").isEqualTo(true)), """
					{
					  "filter": {
					    "property": "Task completed",
					    "checkbox": {
					      "equals": true
					    }
					  }
					}
					"""),
			arguments( // @formatter:off
					where().checkbox("Complete").isEqualTo(true)
						.and(where().select("Language").isEmpty().or().select("Language").isEqualTo("Java")),
					// @formatter:on
					"""
							{
							  "filter": {
							    "and": [
							      {
							        "property": "Complete",
							        "checkbox": {
							          "equals": true
							        }
							      },
							      {
							        "or": [
							          {
							            "property": "Language",
							            "select": {
							              "is_empty": true
							            }
							          },
							          {
							            "property": "Language",
							            "select": {
							              "equals": "Java"
							            }
							          }
							        ]
							      }
							    ]
							  }
							}
							"""),
			arguments( // @formatter:off
					where(where().checkbox("Complete").isEqualTo(true)
							.or().select("Language").isNotEmpty())
						.and().select("Backend framework").isEmpty(),
					// @formatter:on
					"""
							{
							  "filter": {
							    "and": [
							      {
							        "or": [
							          {
							            "property": "Complete",
							            "checkbox": {
							              "equals": true
							            }
							          },
							          {
							            "property": "Language",
							            "select": {
							              "is_not_empty": true
							            }
							          }
							        ]
							      },
							      {
							        "property": "Backend framework",
							        "select": {
							          "is_empty": true
							        }
							      }
							    ]
							  }
							}
							"""));

}
