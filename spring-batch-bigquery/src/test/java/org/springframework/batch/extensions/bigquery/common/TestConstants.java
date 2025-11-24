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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.batch.extensions.bigquery.common.generated.PersonAvroDto;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.core.convert.converter.Converter;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public final class TestConstants {

	private TestConstants() {
	}

	public static final String DATASET = "spring_batch_extensions";

	public static final String PROJECT = "batch-test";

	public static final String NAME = "name";

	public static final String AGE = "age";

	public static final String CSV = "csv";

	public static final String JSON = "json";

	public static final String PARQUET = "parquet";

	private static final String PERSON_1_NAME = "Volodymyr";

	private static final int PERSON_1_AGE = 27;

	private static final String PERSON_2_NAME = "Oleksandra";

	private static final int PERSON_2_AGE = 26;

	public static final Converter<FieldValueList, PersonDto> PERSON_MAPPER = res -> new PersonDto(
			res.get(NAME).getStringValue(), res.get(AGE).getNumericValue().intValue());

	/** Order must be defined so later executed queries results could be predictable */
	private static final List<PersonDto> JAVA_RECORD_PERSONS = Stream
		.of(new PersonDto(PERSON_1_NAME, PERSON_1_AGE), new PersonDto(PERSON_2_NAME, PERSON_2_AGE))
		.sorted(Comparator.comparing(PersonDto::name))
		.toList();

	private static final List<GenericRecord> AVRO_GENERIC_PERSONS = List.of(
			new GenericRecordBuilder(PersonDto.getAvroSchema()).set(NAME, PERSON_1_NAME).set(AGE, PERSON_1_AGE).build(),
			new GenericRecordBuilder(PersonDto.getAvroSchema()).set(NAME, PERSON_2_NAME)
				.set(AGE, PERSON_2_AGE)
				.build());

	private static final List<PersonAvroDto> AVRO_GENERATED_PERSONS = List.of(
			PersonAvroDto.newBuilder().setName(PERSON_1_NAME).setAge(PERSON_1_AGE).build(),
			PersonAvroDto.newBuilder().setName(PERSON_2_NAME).setAge(PERSON_2_AGE).build());

	public static final Chunk<PersonDto> JAVA_RECORD_CHUNK = new Chunk<>(JAVA_RECORD_PERSONS);

	public static final Chunk<GenericRecord> AVRO_GENERIC_CHUNK = new Chunk<>(AVRO_GENERIC_PERSONS);

	public static final Chunk<PersonAvroDto> AVRO_GENERATED_CHUNK = new Chunk<>(AVRO_GENERATED_PERSONS);

}