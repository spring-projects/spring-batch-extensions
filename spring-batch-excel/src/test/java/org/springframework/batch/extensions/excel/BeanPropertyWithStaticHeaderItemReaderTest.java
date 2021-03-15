/*
 * Copyright 2006-2021 the original author or authors.
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

package org.springframework.batch.extensions.excel;

import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.batch.extensions.excel.mapping.BeanWrapperRowMapper;
import org.springframework.batch.extensions.excel.support.rowset.DefaultRowSetFactory;
import org.springframework.batch.extensions.excel.support.rowset.StaticColumnNameExtractor;
import org.springframework.batch.item.ExecutionContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marten Deinum
 * @since 0.1.0
 */
public class BeanPropertyWithStaticHeaderItemReaderTest {

	private MockExcelItemReader<Player> reader;

	@BeforeEach
	public void setup() throws Exception {
		ExecutionContext executionContext = new ExecutionContext();

		List<String[]> rows = new ArrayList<>();
		rows.add(new String[] { "AbduKa00", "Abdul-Jabbar", "Karim", "rb", "1974", "1996" });
		rows.add(new String[] { "AbduRa00", "Abdullah", "Rabih", "rb", "1975", "1999" });
		MockSheet sheet = new MockSheet("players", rows);

		this.reader = new MockExcelItemReader<>(sheet);

		BeanWrapperRowMapper<Player> rowMapper = new BeanWrapperRowMapper<>();
		rowMapper.setTargetType(Player.class);
		rowMapper.afterPropertiesSet();

		this.reader.setRowMapper(rowMapper);

		DefaultRowSetFactory factory = new DefaultRowSetFactory();
		factory.setColumnNameExtractor(new StaticColumnNameExtractor(
				new String[] { "id", "lastName", "firstName", "position", "birthYear", "debutYear" }));
		this.reader.setRowSetFactory(factory);
		this.reader.afterPropertiesSet();
		this.reader.open(executionContext);
	}

	@Test
	public void readandMapPlayers() throws Exception {
		Player p1 = this.reader.read();
		Player p2 = this.reader.read();
		Player p3 = this.reader.read();
		assertThat(p1).isNotNull();
		assertThat(p2).isNotNull();
		assertThat(p3).isNull();

		SoftAssertions softly = new SoftAssertions();

		// Check first player
		softly.assertThat(p1.getId()).isEqualTo("AbduKa00");
		softly.assertThat("Abdul-Jabbar").isEqualTo(p1.getLastName());
		softly.assertThat("Karim").isEqualTo(p1.getFirstName());
		softly.assertThat("rb").isEqualTo(p1.getPosition());
		softly.assertThat(1974).isEqualTo(p1.getBirthYear());
		softly.assertThat(1996).isEqualTo(p1.getDebutYear());
		// Check second player
		softly.assertThat("AbduRa00").isEqualTo(p2.getId());
		softly.assertThat("Abdullah").isEqualTo(p2.getLastName());
		softly.assertThat("Rabih").isEqualTo(p2.getFirstName());
		softly.assertThat("rb").isEqualTo(p2.getPosition());
		softly.assertThat(1975).isEqualTo(p2.getBirthYear());
		softly.assertThat(1999).isEqualTo(p2.getDebutYear());

		softly.assertAll();

	}

}
