/*
 * Copyright 2002-2026 the original author or authors.
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

package org.springframework.batch.extensions.excel.mapping;

import java.util.ArrayList;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

import org.springframework.batch.extensions.excel.MockSheet;
import org.springframework.batch.extensions.excel.Player;
import org.springframework.batch.extensions.excel.support.rowset.DefaultRowSetFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.ParameterizedTypeReference;

/**
 * @author Marten Deinum
 * @since 0.1.0
 */
class BeanWrapperRowMapperTests {

	@Test
	void givenNoNameWhenInitCompleteThenIllegalStateShouldOccur() {
		Assertions.assertThatThrownBy(() -> {
			var mapper = new BeanWrapperRowMapper<Player>();
			mapper.afterPropertiesSet();
		}).isInstanceOf(IllegalStateException.class);
	}

	@Test
	void givenAValidRowWhenMappingThenAValidPlayerShouldBeConstructed() throws Exception {
		var mapper = new BeanWrapperRowMapper<Player>();
		mapper.setTargetType(Player.class);
		mapper.afterPropertiesSet();

		var rows = new ArrayList<String[]>();
		rows.add(new String[] { "id", "lastName", "firstName", "position", "birthYear", "debutYear" });
		rows.add(new String[] { "AbduKa00", "Abdul-Jabbar", "Karim", "rb", "1974", "1996" });
		MockSheet sheet = new MockSheet("players", rows);

		var rs = new DefaultRowSetFactory().create(sheet);
		rs.next();
		rs.next();

		var p = mapper.mapRow(rs);

		var softly = new SoftAssertions();
		softly.assertThat(p).isNotNull();
		softly.assertThat(p.getId()).isEqualTo("AbduKa00");
		softly.assertThat("Abdul-Jabbar").isEqualTo(p.getLastName());
		softly.assertThat("Karim").isEqualTo(p.getFirstName());
		softly.assertThat("rb").isEqualTo(p.getPosition());
		softly.assertThat(1974).isEqualTo(p.getBirthYear());
		softly.assertThat(1996).isEqualTo(p.getDebutYear());
		softly.assertThat(p.getComment()).isNull();
		softly.assertAll();

	}

	@Test
	void givenAValidRowWhenMappingThenAValidPlayerShouldBeConstructedBasedOnPrototype() throws Exception {

		var ctx = new AnnotationConfigApplicationContext(TestConfig.class);
		var mapper = ctx.getBeanProvider(new ParameterizedTypeReference<BeanWrapperRowMapper<Player>>() {
		}).getIfAvailable();

		var rows = new ArrayList<String[]>();
		rows.add(new String[] { "id", "lastName", "firstName", "position", "birthYear", "debutYear" });
		rows.add(new String[] { "AbduKa00", "Abdul-Jabbar", "Karim", "rb", "1974", "1996" });
		MockSheet sheet = new MockSheet("players", rows);

		var rs = new DefaultRowSetFactory().create(sheet);
		rs.next();
		rs.next();
		var p = mapper.mapRow(rs);

		var softly = new SoftAssertions();
		softly.assertThat(p).isNotNull();
		softly.assertThat(p.getId()).isEqualTo("AbduKa00");
		softly.assertThat("Abdul-Jabbar").isEqualTo(p.getLastName());
		softly.assertThat("Karim").isEqualTo(p.getFirstName());
		softly.assertThat("rb").isEqualTo(p.getPosition());
		softly.assertThat(1974).isEqualTo(p.getBirthYear());
		softly.assertThat(1996).isEqualTo(p.getDebutYear());
		softly.assertThat(p.getComment()).isEqualTo("comment from context");
		softly.assertAll();

	}

	@Configuration
	public static class TestConfig {

		@Bean
		public BeanWrapperRowMapper<Player> playerRowMapper() {
			var mapper = new BeanWrapperRowMapper<Player>();
			mapper.setPrototypeBeanName("player");
			return mapper;
		}

		@Bean
		@Scope("prototype")
		public Player player() {
			var p = new Player();
			p.setComment("comment from context");
			return p;
		}
	}
}
