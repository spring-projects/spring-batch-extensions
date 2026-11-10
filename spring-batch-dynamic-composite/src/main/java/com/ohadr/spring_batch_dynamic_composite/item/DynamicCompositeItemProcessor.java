package com.ohadr.spring_batch_dynamic_composite.item;

import java.util.ArrayList;
import java.util.List;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import com.ohadr.spring_batch_dynamic_composite.core.BatchBeanTypeEnum;
import com.ohadr.spring_batch_dynamic_composite.core.CompositeBatchBeanEntity;

public class DynamicCompositeItemProcessor<I, O> extends AbstractDynamicCompositeItem 
	implements ItemProcessor<I, O>
{
	//aggregate CompositeItemProcessor<I, O> rather than extending it:
	protected CompositeItemProcessor<I, O> innerComposite = new CompositeItemProcessor<I, O>();
	
	protected void getProcessorsList()
	{
		List<ItemProcessor<I, O>> delegates = new ArrayList<ItemProcessor<I, O>>();

		BatchBeanTypeEnum batchBeanType = BatchBeanTypeEnum.PROCESSOR;
		List<CompositeBatchBeanEntity> processorsList = compositeBatchBeanManager.getBatchBeanList(taskName, batchBeanType);
		if (processorsList.isEmpty() && !acceptEmptyFiltersList)
		{
			String message = "No " + batchBeanType + " were found for taskName=" + taskName;
			throw new RuntimeException(message);
		}

		for (CompositeBatchBeanEntity compositeProcessorEntity : processorsList)
		{
			// load generic filter by name
			ItemProcessor<I, O> processor = applicationContext.getBean(compositeProcessorEntity.getName(), ItemProcessor.class);
			delegates.add( processor );
		}

		innerComposite.setDelegates(delegates);
	}

	@Override
	protected void getItemsList()
	{
		getProcessorsList();	
	}

	@Override
	public O process(I item) throws Exception
	{
		return innerComposite.process(item);
	}
}
