package org.springframework.batch.item.excel.mapping;

import org.junit.Test;
import org.springframework.batch.item.Player;
import org.springframework.batch.item.excel.MockSheet;
import org.springframework.batch.item.excel.support.rowset.DefaultRowSetFactory;
import org.springframework.batch.item.excel.support.rowset.RowSet;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by in329dei on 17-9-2014.
 */
public class BeanPropertyRowMapperTest {

    @Test(expected = IllegalStateException.class)
    public void givenNoNameWhenInitCompleteThenIllegalStateShouldOccur() throws Exception {
        BeanPropertyRowMapper mapper = new BeanPropertyRowMapper();
        mapper.afterPropertiesSet();
    }

    @Test
    public void givenAValidRowWhenMappingThenAValidPlayerShouldBeConstructed() throws Exception {
        BeanPropertyRowMapper<Player> mapper = new BeanPropertyRowMapper<Player>();
        mapper.setTargetType(Player.class);
        mapper.afterPropertiesSet();

        List<String[]> rows = new ArrayList<String[]>();
        rows.add(new String[]{"id", "lastName", "firstName", "position", "birthYear", "debutYear"});
        rows.add( new String[]{"AbduKa00", "Abdul-Jabbar", "Karim", "rb", "1974", "1996"});
        MockSheet sheet = new MockSheet("players", rows);


        RowSet rs = new DefaultRowSetFactory().create(sheet);
        rs.next();
        rs.next();

        Player p = mapper.mapRow(rs);
        assertNotNull(p);
        assertEquals("AbduKa00", p.getId());
        assertEquals("Abdul-Jabbar", p.getLastName());
        assertEquals("Karim", p.getFirstName());
        assertEquals("rb", p.getPosition());
        assertEquals(1974, p.getBirthYear());
        assertEquals(1996, p.getDebutYear());
        assertNull(p.getComment());

    }

    @Test
    public void givenAValidRowWhenMappingThenAValidPlayerShouldBeConstructedBasedOnPrototype() throws Exception {

        ApplicationContext ctx = new AnnotationConfigApplicationContext(TestConfig.class);
        BeanPropertyRowMapper<Player> mapper = new BeanPropertyRowMapper<Player>();
        mapper.setPrototypeBeanName("player");
        mapper.setBeanFactory(ctx);
        mapper.afterPropertiesSet();

        List<String[]> rows = new ArrayList<String[]>();
        rows.add(new String[]{"id", "lastName", "firstName", "position", "birthYear", "debutYear"});
        rows.add( new String[]{"AbduKa00", "Abdul-Jabbar", "Karim", "rb", "1974", "1996"});
        MockSheet sheet = new MockSheet("players", rows);

        RowSet rs = new DefaultRowSetFactory().create(sheet);
        rs.next();
        rs.next();
        Player p = mapper.mapRow(rs);

        assertNotNull(p);
        assertEquals("AbduKa00", p.getId());
        assertEquals("Abdul-Jabbar", p.getLastName());
        assertEquals("Karim", p.getFirstName());
        assertEquals("rb", p.getPosition());
        assertEquals(1974, p.getBirthYear());
        assertEquals(1996, p.getDebutYear());
        assertEquals("comment from context", p.getComment());
    }

    @Configuration
    public static class TestConfig {

        @Bean
        @Scope(value = "prototype")
        public Player player() {
            Player p = new Player();
            p.setComment("comment from context");
            return p;
        }

    }
}
