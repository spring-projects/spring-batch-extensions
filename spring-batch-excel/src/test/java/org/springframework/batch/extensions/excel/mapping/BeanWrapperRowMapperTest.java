/*
 * Copyright 2002-2014 the original author or authors.
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
import java.util.List;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

import org.springframework.batch.extensions.excel.MockSheet;
import org.springframework.batch.extensions.excel.Player;
import org.springframework.batch.extensions.excel.support.rowset.DefaultRowSetFactory;
import org.springframework.batch.extensions.excel.support.rowset.RowSet;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * @author Marten Deinum
 * @since 0.1.0
 */
public class BeanWrapperRowMapperTest {

	@Test
	public void givenNoNameWhenInitCompleteThenIllegalStateShouldOccur() {
		Assertions.assertThatThrownBy(() -> {
			BeanWrapperRowMapper<Player> mapper = new BeanWrapperRowMapper<>();
			mapper.afterPropertiesSet();
		}).isInstanceOf(IllegalStateException.class);
	}

	@Test
	public void givenAValidRowWhenMappingThenAValidPlayerShouldBeConstructed() throws Exception {
		BeanWrapperRowMapper<Player> mapper = new BeanWrapperRowMapper<>();
		mapper.setTargetType(Player.class);
		mapper.afterPropertiesSet();

		List<String[]> rows = new ArrayList<>();
		rows.add(new String[] { "id", "lastName", "firstName", "position", "birthYear", "debutYear" });
		rows.add(new String[] { "AbduKa00", "Abdul-Jabbar", "Karim", "rb", "1974", "1996" });
		MockSheet sheet = new MockSheet("players", rows);

		RowSet rs = new DefaultRowSetFactory().create(sheet);
		rs.next();
		rs.next();

		Player p = mapper.mapRow(rs);

		SoftAssertions softly = new SoftAssertions();
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
	public void givenAValidRowWhenMappingThenAValidPlayerShouldBeConstructedBasedOnPrototype() throws Exception {

		ApplicationContext ctx = new AnnotationConfigApplicationContext(TestConfig.class);
		BeanWrapperRowMapper<Player> mapper = ctx.getBean("playerRowMapper", BeanWrapperRowMapper.class);

		List<String[]> rows = new ArrayList<>();
		rows.add(new String[] { "id", "lastName", "firstName", "position", "birthYear", "debutYear" });
		rows.add(new String[] { "AbduKa00", "Abdul-Jabbar", "Karim", "rb", "1974", "1996" });
		MockSheet sheet = new MockSheet("players", rows);

		RowSet rs = new DefaultRowSetFactory().create(sheet);
		rs.next();
		rs.next();
		Player p = mapper.mapRow(rs);

		SoftAssertions softly = new SoftAssertions();
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
			BeanWrapperRowMapper<Player> mapper = new BeanWrapperRowMapper<>();
			mapper.setPrototypeBeanName("player");
			return mapper;
		}

		@Bean
		@Scope("prototype")
		public Player player() {
			Player p = new Player();
			p.setComment("comment from context");
			return p;
		}

	}

}
