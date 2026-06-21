package com.ohadr.spring_batch_dynamic_composite.repository;

import java.util.List;
import java.util.Set;
import com.ohadr.spring_batch_dynamic_composite.core.BatchBeanTypeEnum;
import com.ohadr.spring_batch_dynamic_composite.core.CompositeBatchBeanEntity;

public interface CompositeBatchBeanDao 
{

	List<CompositeBatchBeanEntity> getCompositeBatchBeans(String taskName, BatchBeanTypeEnum batchBeanType);

	Set<String> getAllTaskNames();

	Set<String> getAllValuesOfBeanTypes();

	void update(CompositeBatchBeanEntity processor);

	void delete(Long compositeBatchBeanId);

	CompositeBatchBeanEntity get(Long compositeBatchBeanId);

	void setTablePrefix(String tablePrefix);

}
