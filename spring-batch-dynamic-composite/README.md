# spring-batch-dynamic-composite 

This project is all about spring-batch' composite processor and writer. Currently, spring-batch' built-in composite processor and writer are not dynamic. Meaning, one set them in design time (and coding) and it cannot be changed during runtime. But there are cases that this is needed.

Without loss of generality, we say here "processor" but mean "reader" and "writer" as well.

The idea is to have the ability to replace a processor(s) at runtime. For example, in case of multiple processors (AKA composite-processor), to have the option to add/remove/replace/change-order of processors.

In the implementation, there is `DynamicCompositeItemProcessor` (which is a real `ItemProcessor`) that uses a manager to read the list of processors bean-names from the DB. Thus, the processors list can be modified and reloaded.

As mentioned, same for reader as well as for writer.
 
## why do we need this?

There are cases that processors are used as "filters", and it may occur that the business (the client) may change the requirements (yes, it is very annoying) and ask to switch among filters (change the priority). 

Other use case is having multiple readers, reading the data from different data warehouses, and again - the client changes the warehouse from time to time (integration phase), and I do not want my app to be restarted each and every time. 

There are many other use cases, of course.
