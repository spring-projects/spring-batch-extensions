package org.springframework.batch.item.file;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class FileWriterTemplate {
	
	private File file;
	
	public FileWriterTemplate(File file) {
		this.file = file;
	}
	
	public void write(final FileWriterAction action) throws IOException {
		
		Writer w = new FileWriter(file, false);
		BufferedWriter bw = new BufferedWriter(w);
		
		try {
			
			action.write(bw);
			
			
		} finally {
			bw.close();
			w.close();
		}
	}
}
