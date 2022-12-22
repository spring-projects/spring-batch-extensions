/*
 * Copyright 2002-2022 the original author or authors.
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

package org.springframework.batch.extensions.bigquery.common;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;

@JsonPropertyOrder(value = {"name", "age"})
public record PersonDto(String name, Integer age) {

    public static Schema getBigQuerySchema() {
        Field nameField = Field.newBuilder("name", StandardSQLTypeName.STRING).build();
        Field ageField = Field.newBuilder("age", StandardSQLTypeName.INT64).build();
        return Schema.of(nameField, ageField);
    }

}
