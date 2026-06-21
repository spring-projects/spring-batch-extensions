package com.ohadr.spring_batch_dynamic_composite.item;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.StringUtils;
import com.ohadr.spring_batch_dynamic_composite.core.CompositeBatchBeanManager;

public abstract class AbstractDynamicCompositeItem
	implements 
	StepExecutionListener, 
	ApplicationContextAware, 
	InitializingBean
{
	protected String taskName;

	@Autowired
	protected CompositeBatchBeanManager compositeBatchBeanManager;

	// if false, filter result is false if any filter is defined
	protected Boolean acceptEmptyFiltersList = false;

	protected ApplicationContext applicationContext;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException
	{
		this.applicationContext = applicationContext;
	}

	//hide super's implementation
	@Override
	public void afterPropertiesSet() throws Exception 
	{
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution)
	{
		return stepExecution != null ? stepExecution.getExitStatus() : null;
	}

	/**
	 * get items list, which means processors, readers or writers. each implemetation should
	 * set the list that is read from DB to the "delegates" member (e.g. CompositeItemProcessor.delegates)
	 */
	protected abstract void getItemsList();

	@Override
	public void beforeStep(StepExecution stepExecution)
	{
		taskName = stepExecution.getJobExecution().getJobInstance().getJobName();
		
		if (StringUtils.isEmpty(taskName))
		{
			String message = getClass().getSimpleName() + " beforeStep: taskName is null or empty.";
	//		Log.error(message);
			throw new RuntimeException(message);
		}
		
		//get processors list from DB
		getItemsList();
	}
}
