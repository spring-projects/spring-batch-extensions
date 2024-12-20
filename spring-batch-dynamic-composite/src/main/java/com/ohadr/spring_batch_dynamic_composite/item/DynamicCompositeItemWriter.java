package com.ohadr.spring_batch_dynamic_composite.item;

import java.util.ArrayList;
import java.util.List;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import com.ohadr.spring_batch_dynamic_composite.core.BatchBeanTypeEnum;
import com.ohadr.spring_batch_dynamic_composite.core.CompositeBatchBeanEntity;

public class DynamicCompositeItemWriter<T> extends AbstractDynamicCompositeItem 
	implements ItemWriter<T>
{
	//aggregate CompositeItemWriter<T> rather than extending it:
	protected CompositeItemWriter<T>	innerComposite = new CompositeItemWriter<>();

	protected void getWritersList()
	{
		List<ItemWriter<? super T>> delegates = new ArrayList<ItemWriter<? super T>>();

		BatchBeanTypeEnum batchBeanType = BatchBeanTypeEnum.WRITER;
		List<CompositeBatchBeanEntity> processorsList = compositeBatchBeanManager.getBatchBeanList(taskName, batchBeanType);
		if (processorsList.isEmpty() && !acceptEmptyFiltersList)
		{
			String message = "No " + batchBeanType + " were found for taskName=" + taskName;
			throw new RuntimeException(message);
		}

		for (CompositeBatchBeanEntity compositeProcessorEntity : processorsList)
		{
			// load generic filter by name
			ItemWriter<T> writer = applicationContext.getBean(compositeProcessorEntity.getName(), ItemWriter.class);
			delegates.add( writer );
		}

		innerComposite.setDelegates(delegates);
	}

	@Override
	protected void getItemsList()
	{
		getWritersList();	
	}

	@Override
	public void write(List<? extends T> items) throws Exception
	{
		innerComposite.write(items);
	}

}
