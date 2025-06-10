/*
 * Copyright 2002-2025 the original author or authors.
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

import com.google.cloud.bigquery.FieldValueList;
import org.springframework.batch.item.Chunk;
import org.springframework.core.convert.converter.Converter;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public final class TestConstants {

    private TestConstants() {}

    public static final String DATASET = "spring_batch_extensions";
    public static final String NAME = "name";
    public static final String AGE = "age";
    public static final String CSV = "csv";
    public static final String JSON = "json";

    public static final Converter<FieldValueList, PersonDto> PERSON_MAPPER = res -> new PersonDto(
            res.get(NAME).getStringValue(), res.get(AGE).getNumericValue().intValue()
    );

    /** Order must be defined so later executed queries results could be predictable */
    private static final List<PersonDto> PERSONS = Stream
            .of(new PersonDto("Volodymyr", 27), new PersonDto("Oleksandra", 26))
            .sorted(Comparator.comparing(PersonDto::name))
            .toList();

    public static final Chunk<PersonDto> CHUNK = new Chunk<>(PERSONS);

}