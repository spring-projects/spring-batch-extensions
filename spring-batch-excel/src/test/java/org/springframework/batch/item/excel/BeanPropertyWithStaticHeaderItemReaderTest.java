package org.springframework.batch.item.excel;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.Player;
import org.springframework.batch.item.excel.mapping.BeanWrapperRowMapper;
import org.springframework.batch.item.excel.support.rowset.DefaultRowSetFactory;
import org.springframework.batch.item.excel.support.rowset.StaticColumnNameExtractor;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by in329dei on 17-9-2014.
 */
public class BeanPropertyWithStaticHeaderItemReaderTest {

    private MockExcelItemReader<Player> reader;

    private ExecutionContext executionContext;

    @Before
    public void setup() throws Exception {
        executionContext = new ExecutionContext();

        List<String[]> rows = new ArrayList<String[]>();
        rows.add( new String[]{"AbduKa00", "Abdul-Jabbar", "Karim", "rb", "1974", "1996"});
        rows.add( new String[]{"AbduRa00", "Abdullah", "Rabih", "rb", "1975", "1999"});
        MockSheet sheet = new MockSheet("players", rows);

        reader = new MockExcelItemReader<Player>(sheet);

        BeanWrapperRowMapper<Player> rowMapper = new BeanWrapperRowMapper<Player>();
        rowMapper.setTargetType(Player.class);
        rowMapper.afterPropertiesSet();

        reader.setRowMapper(rowMapper);

        DefaultRowSetFactory factory = new DefaultRowSetFactory();
        factory.setColumnNameExtractor(new StaticColumnNameExtractor(new String[]{"id", "lastName", "firstName", "position", "birthYear", "debutYear"}));
        reader.setRowSetFactory(factory);
        reader.afterPropertiesSet();
        reader.open(executionContext);
    }

    @Test
    public void readandMapPlayers() throws Exception {
        Player p1 = reader.read();
        Player p2 = reader.read();
        Player p3 = reader.read();
        assertNotNull(p1);
        assertNotNull(p2);
        assertNull(p3);

        // Check first player
        assertEquals("AbduKa00", p1.getId());
        assertEquals("Abdul-Jabbar", p1.getLastName());
        assertEquals("Karim", p1.getFirstName());
        assertEquals("rb", p1.getPosition());
        assertEquals(1974, p1.getBirthYear());
        assertEquals(1996, p1.getDebutYear());
        // Check second player
        assertEquals("AbduRa00", p2.getId());
        assertEquals("Abdullah", p2.getLastName());
        assertEquals("Rabih", p2.getFirstName());
        assertEquals("rb", p2.getPosition());
        assertEquals(1975, p2.getBirthYear());
        assertEquals(1999, p2.getDebutYear());
    }

}
