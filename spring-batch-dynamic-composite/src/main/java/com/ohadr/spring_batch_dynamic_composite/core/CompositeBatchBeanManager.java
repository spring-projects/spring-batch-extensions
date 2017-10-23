package com.ohadr.spring_batch_dynamic_composite.core;

import java.util.List;
import java.util.Set;

public interface CompositeBatchBeanManager
{

	List<CompositeBatchBeanEntity> getBatchBeanList(String task, BatchBeanTypeEnum batchBeanType);

	Set<String> getAllTaskNames();

	Set<String> getAllValuesOfBeanTypes();

	void deleteBatchBean(Long compositeBatchBeanId);

	CompositeBatchBeanEntity getBatchBean(Long compositeBatchBeanId);

	void addBatchBean(CompositeBatchBeanEntity processor);

}
