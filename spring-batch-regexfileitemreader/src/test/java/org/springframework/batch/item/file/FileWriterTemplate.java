/*
 * Copyright 2002-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
