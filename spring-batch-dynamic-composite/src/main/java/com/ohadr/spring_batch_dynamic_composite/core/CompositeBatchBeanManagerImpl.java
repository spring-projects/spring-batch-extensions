/**
 * Copyright (c) 2016, William Hill Online. All rights reserved
 */
package com.ohadr.spring_batch_dynamic_composite.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
//import org.springframework.cache.annotation.Cacheable;
import org.springframework.util.StringUtils;

import com.ohadr.spring_batch_dynamic_composite.repository.CompositeBatchBeanDao;


@Component
public class CompositeBatchBeanManagerImpl implements CompositeBatchBeanManager
{
	private static Logger log = Logger.getLogger(CompositeBatchBeanManagerImpl.class);
	/**
	 * UID for CompositeBatchBeanManager
	 */
	public final static String UID = "compositeBatchBeanManager";

	@Autowired
	private CompositeBatchBeanDao compositeBatchBeanDao;



	@Override
//	@Cacheable(value = "compositeProcessorCache", key = "#key1+#batchBeanType+#key2")
	public List<CompositeBatchBeanEntity> getBatchBeanList(String taskName, BatchBeanTypeEnum batchBeanType)
	{
		if (StringUtils.isEmpty(taskName))
		{
			throw new IllegalArgumentException("taskName is null.");
		}

		if (batchBeanType == null)
		{
			throw new IllegalArgumentException("batchBeanType is null.");
		}

		List<CompositeBatchBeanEntity> batchBeanEntities = compositeBatchBeanDao.getCompositeBatchBeans(taskName, batchBeanType);

		if (batchBeanEntities == null)
		{
			return new ArrayList<>();
		}

		Collections.sort(batchBeanEntities);

		log.info("beans list:" + batchBeanEntities);

		return batchBeanEntities;
	}


	@Override
	public void addBatchBean(CompositeBatchBeanEntity processor)
	{
		if (processor == null)
		{
			throw new IllegalArgumentException("processor is null.");
		}

		compositeBatchBeanDao.update(processor);
	}

	@Override
	public void deleteBatchBean(Long compositeBatchBeanId)
	{
		compositeBatchBeanDao.delete(compositeBatchBeanId);
	}

	@Override
	public CompositeBatchBeanEntity getBatchBean(Long compositeBatchBeanId)
	{
		return compositeBatchBeanDao.get(compositeBatchBeanId);
	}


	@Override
	public Set<String> getAllTaskNames()
	{
		return compositeBatchBeanDao.getAllTaskNames();
	}

	@Override
	public Set<String> getAllValuesOfBeanTypes()
	{
		return compositeBatchBeanDao.getAllValuesOfBeanTypes();
	}
	
	public void setTablePrefix(String tablePrefix)
	{
		compositeBatchBeanDao.setTablePrefix(tablePrefix);
	}

}
