package org.springframework.batch.item.file;

import java.io.IOException;
import java.io.Writer;

public interface FileWriterAction {
	
	void write(Writer writer) throws IOException;

}
