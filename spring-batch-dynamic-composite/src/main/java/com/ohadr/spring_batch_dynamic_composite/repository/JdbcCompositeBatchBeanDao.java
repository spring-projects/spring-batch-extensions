package com.ohadr.spring_batch_dynamic_composite.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import com.ohadr.spring_batch_dynamic_composite.core.BatchBeanTypeEnum;
import com.ohadr.spring_batch_dynamic_composite.core.CompositeBatchBeanEntity;

@Repository
public class JdbcCompositeBatchBeanDao implements CompositeBatchBeanDao
{
	private static Logger log = Logger.getLogger(JdbcCompositeBatchBeanDao.class);

	private static final String TABLE_NAME = "composite_batch_beans";
	private static final String DEFAULT_TABLE_PREFIX = "";
	private String tablePrefix = DEFAULT_TABLE_PREFIX;

	private static final String COMPOSITE_BATCH_BEANS_FIELDS = "ID, "
			+ "NAME, "
			+ "JOB_NAME, "
			+ "BATCH_BEAN_TYPE, "
			+ "PRIORITY";

	private final String DEFAULT_COMPOSITE_BATCH_BEANS_SELECT_STATEMENT = "select " + COMPOSITE_BATCH_BEANS_FIELDS
			+ " from " + getTableName() + " where JOB_NAME = ? and BATCH_BEAN_TYPE = ?";

	private JdbcOperations jdbcTemplate;


	@Autowired
	public void setDataSource(DataSource dataSource)
	{
		jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Override
	public List<CompositeBatchBeanEntity> getCompositeBatchBeans(String taskName, BatchBeanTypeEnum batchBeanType)
	{
		List<CompositeBatchBeanEntity> batchBeans = null;
		try
		{
			log.info("query: " + DEFAULT_COMPOSITE_BATCH_BEANS_SELECT_STATEMENT + " taskName=" + taskName + " batchBeanType=" + batchBeanType);
			batchBeans = jdbcTemplate.query(DEFAULT_COMPOSITE_BATCH_BEANS_SELECT_STATEMENT, 
					new CompositeBatchBeanEntityRowMapper(), 
					taskName, batchBeanType.name());
		}
		catch (EmptyResultDataAccessException e) 
		{
			log.info("No record was found for taskName: " + taskName);
			throw new NoSuchElementException("no record was found for taskName: " + taskName);
		}


		return batchBeans;
	}

	@Override
	public Set<String> getAllTaskNames()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getAllValuesOfBeanTypes()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void update(CompositeBatchBeanEntity processor)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(Long compositeBatchBeanId)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public CompositeBatchBeanEntity get(Long compositeBatchBeanId)
	{
		// TODO Auto-generated method stub
		return null;
	}

	private final class CompositeBatchBeanEntityRowMapper implements RowMapper<CompositeBatchBeanEntity> 
	{
		@Override
		public CompositeBatchBeanEntity mapRow(ResultSet rs, int rowNum) throws SQLException 
		{
			CompositeBatchBeanEntity compositeBatchBeanEntity = 
					new CompositeBatchBeanEntity(rs.getLong(1), rs.getString(2), rowNum, null, null);
			// should always be at version=0 because they never get updated
			compositeBatchBeanEntity.incrementVersion();
			return compositeBatchBeanEntity;
		}
	}
	
	@Override
	public void setTablePrefix(String tablePrefix)
	{
		this.tablePrefix = tablePrefix;
	}

	private String getTableName()
	{
		return tablePrefix + TABLE_NAME;
	}
}
