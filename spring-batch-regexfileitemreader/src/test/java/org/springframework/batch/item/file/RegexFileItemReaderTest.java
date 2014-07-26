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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.RegexLineTokenizer;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

public class RegexFileItemReaderTest {
	
	@Mock
	private LineMapper<TestItem> emptyLineMapper;

	private LineMapper<TestItem> getLineMapperForRegex(final String regex) {
		
		return new DefaultLineMapper<TestItem>() {{
            setLineTokenizer(new RegexLineTokenizer() {{
                setNames(new String[] { "sequenceNumber", "id", "text" });
                setPattern(Pattern.compile(regex, Pattern.DOTALL));
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<TestItem>() {{
                setTargetType(TestItem.class);
            }});
		}};
	}

	private RegexFileItemReader<TestItem> createRegexFileItemReader(final Resource resource, final String regex) throws Exception {
		
		RegexFileItemReader<TestItem> reader = new RegexFileItemReader<TestItem>();
		reader.setLineMapper(getLineMapperForRegex(regex));
		reader.setPattern(Pattern.compile(regex, Pattern.DOTALL));
		reader.setResource(resource);
		reader.afterPropertiesSet();
		
		return reader;
	}
	
	private List<TestItem> readFromReader(final RegexFileItemReader<TestItem> itemReader) throws Exception {
		
		List<TestItem> result = new ArrayList<TestItem>();
		TestItem item;
		
		itemReader.doOpen();
		while((item = itemReader.read()) != null) {
			result.add(item);
		}
		itemReader.doClose();
		
		return result;
	}
	
	private File createTestFile() throws IOException {
		
		File f = File.createTempFile("batchTest", ".html");
		f.deleteOnExit();
				
		return f;
	}
	
	private void checkSequenceNumber(List<TestItem> list) {
		for(int i=0; i < list.size(); i++) {
			assertEquals(i, list.get(i).getSequenceNumber());
		}
	}
	
	@Before
	public void setUp() throws Exception {
		initMocks(this);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void shouldFailAssertionOnNullLineMapper() throws Exception {

		try {
			RegexFileItemReader<TestItem> reader = new RegexFileItemReader<TestItem>();
			reader.setLineMapper(null);
			reader.afterPropertiesSet();
			fail("Assertion should have thrown exception on null LineMapper");
		} catch (Exception e) {
			assertEquals("LineMapper is required", e.getMessage());
			throw e;
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldFailAssertionOnNullPattern() throws Exception {

		try {
			RegexFileItemReader<TestItem> reader = new RegexFileItemReader<TestItem>();
			reader.setLineMapper(emptyLineMapper);
			reader.afterPropertiesSet();
			fail("Assertion should have thrown exception on null Pattern");
		} catch (Exception e) {
			assertEquals("Pattern is required", e.getMessage());
			throw e;
		}
	}

	@Test
	public void testHtmlSimpleTable() throws Exception {

		/** Test against html table like this
		 <html><table>
			<tr>
			<td>0</td>
			<td id=ID0>other</td>
			</tr>
			<tr>
			<td>1</td>
			<td id=ID1>other</td>
			</tr>
			...
			<br>
			</table></html> 
		 */
		
		File f = createTestFile();
		
		FileWriterTemplate template = new FileWriterTemplate(f);
		template.write(new FileWriterAction() {

			@Override
			public void write(Writer writer) throws IOException {

				writer.write("<html><table>\n");
				for(int i=0; i < 100; i++) {
					writer.write("<tr>\n");
					writer.write("<td>"+i+"</td>\n");
					writer.write("<td id=ID"+i+">other</td>\n");
					writer.write("</tr>\n");
				}
				writer.write("<br>");
				writer.write("</table></html>");

			}
			
		});
		
		String regex = "<tr>.*?<td>(.*?)</td>.*?<td id=(.*?)>(.*?)</td>.*?</tr>";
		
		RegexFileItemReader<TestItem> itemReader = createRegexFileItemReader(new FileSystemResource(f), regex);
		List<TestItem> result = readFromReader(itemReader);
		assertEquals(100, result.size());
		checkSequenceNumber(result);
		assertEquals("other", result.get(0).getText());
	}
	
	@Test
	public void testHtmlSimpleTableWithFirstItemAfterBufferSize() throws Exception {

		/** Test against html table like this
		 <html><table>
		 	... spaces ...
			<tr>
			<td>0</td>
			<td id=ID0>other</td>
			</tr>
			<tr>
			<td>1</td>
			<td id=ID1>other</td>
			</tr>
			...
			<br>
			</table></html> 
		 */

		File f = createTestFile();

		FileWriterTemplate template = new FileWriterTemplate(f);
		template.write(new FileWriterAction() {

			@Override
			public void write(Writer writer) throws IOException {

				writer.write("<html><table>\n");
				for(int i=0; i < 2048; i++) {
					writer.write(" ");
				}
				for(int i=0; i < 100; i++) {
					writer.write("<tr>\n");
					writer.write("<td>"+i+"</td>\n");
					writer.write("<td id=ID"+i+">other</td>\n");
					writer.write("</tr>\n");
				}
				writer.write("<br>");
				writer.write("</table></html>");

			}
			
		});
		
		String regex = "<tr>.*?<td>(.*?)</td>.*?<td id=(.*?)>(.*?)</td>.*?</tr>";
		
		RegexFileItemReader<TestItem> itemReader = createRegexFileItemReader(new FileSystemResource(f), regex);
		List<TestItem> result = readFromReader(itemReader);
		assertEquals(100, result.size());
		checkSequenceNumber(result);
		assertEquals("other", result.get(0).getText());
	}

	@Test
	public void testHtmlSimpleTableWithNoItem() throws Exception {

		/** Test against html table like this
		 <html><table>
		 	... other content ...
		 </table></html> 
		 */

		File f = createTestFile();

		FileWriterTemplate template = new FileWriterTemplate(f);
		template.write(new FileWriterAction() {

			@Override
			public void write(Writer writer) throws IOException {

				writer.write("<html><table>\n");
				for(int i=0; i < 10; i++) {
					writer.write("<tr>\n");
					writer.write("<td>other</td>\n");
					writer.write("</tr>\n");
				}
				writer.write("<br>");
				writer.write("</table></html>");

			}
			
		});
		
		String regex = "<tr>.*?<td>(.*?)</td>.*?<td id=(.*?)>(.*?)</td>.*?</tr>";
		
		RegexFileItemReader<TestItem> itemReader = createRegexFileItemReader(new FileSystemResource(f), regex);
		List<TestItem> result = readFromReader(itemReader);
		assertEquals(0, result.size());
	}
	
	@Test
	public void testHtmlGithubIssueList() throws Exception {

		String regex = "<li id=\"issue_(.*?)\".*?>.*?<h4.*?>.*?<a href=\"(.*?)\".*?>(.*?)</a>.*?</h4>";
		
		RegexFileItemReader<TestItem> itemReader = createRegexFileItemReader(new ClassPathResource("TestHtmlGithubIssueList.txt", RegexFileItemReaderTest.class), regex);
		List<TestItem> result = readFromReader(itemReader);
		assertEquals(3, result.size());
		assertEquals("Update README.md and add CONTRIBUTING.md", result.get(2).getText());
		assertEquals(1, result.get(2).getSequenceNumber());
		assertEquals("/spring-projects/spring-batch-extensions/pull/1", result.get(2).getId());
	}
	
	@Test
	public void testHtmlJiraIssueList() throws Exception {

		String regex = "<li.*?data-id=\"(.*?)\" data-key=\"(.*?)\" title=\"(.*?)\">.*?</li>";
		
		RegexFileItemReader<TestItem> itemReader = createRegexFileItemReader(new ClassPathResource("TestHtmlJiraIssueList.txt", RegexFileItemReaderTest.class), regex);
		List<TestItem> result = readFromReader(itemReader);
		assertEquals(50, result.size());
		assertEquals(58901, result.get(0).getSequenceNumber());
		assertEquals("BATCH-2276", result.get(0).getId());
		assertEquals(52006, result.get(1).getSequenceNumber());
		assertEquals("BATCH-2147", result.get(1).getId());
		assertEquals(36034, result.get(49).getSequenceNumber());
		assertEquals("BATCH-1686", result.get(49).getId());
	}
	
	@Test
	public void testReaderItemCountProperty() throws Exception {

		File f = createTestFile();

		FileWriterTemplate template = new FileWriterTemplate(f);
		template.write(new FileWriterAction() {

			@Override
			public void write(Writer writer) throws IOException {

				for(int i=0; i < 100; i++) {
					writer.write("<tr><td>"+i+"</td><td id=ID"+i+">other</td></tr>");
				}
			}
		});
		
		String regex = "<tr>.*?<td>(.*?)</td>.*?<td id=(.*?)>(.*?)</td>.*?</tr>";

		final AtomicInteger testCounter = new AtomicInteger(1);
		RegexFileItemReader<TestItem> itemReader = createRegexFileItemReader(new FileSystemResource(f), regex);
		itemReader.setLineMapper(new LineMapper<TestItem>() {

			@Override
			public TestItem mapLine(String line, int lineNumber)
					throws Exception {
				
				assertEquals(testCounter.getAndIncrement(), lineNumber);
				return new TestItem();
			}
			
		});
		
		List<TestItem> result = readFromReader(itemReader);
		assertEquals(100, result.size());
	}
	
	@Test
	public void testReaderMaxItemCountProperty() throws Exception {

		File f = createTestFile();

		FileWriterTemplate template = new FileWriterTemplate(f);
		template.write(new FileWriterAction() {

			@Override
			public void write(Writer writer) throws IOException {

				for(int i=0; i < 100; i++) {
					writer.write("<tr><td>"+i+"</td><td id=ID"+i+">other</td></tr>");
				}
			}
		});
		
		String regex = "<tr>.*?<td>(.*?)</td>.*?<td id=(.*?)>(.*?)</td>.*?</tr>";

		RegexFileItemReader<TestItem> itemReader = createRegexFileItemReader(new FileSystemResource(f), regex);
		itemReader.setMaxItemCount(10);
		List<TestItem> result = readFromReader(itemReader);
		assertEquals(10, result.size());
	}
	
	@Test
	public void testFirstItemBetweenFirstAndSecondBlock() throws Exception {

		File f = createTestFile();

		FileWriterTemplate template = new FileWriterTemplate(f);
		template.write(new FileWriterAction() {

			@Override
			public void write(Writer writer) throws IOException {

				for(int i=0; i < 2048-4; i++) {
					writer.write("-");
				}

				for(int i=0; i < 100; i++) {
					writer.write("<tr><td>"+i+"</td><td id=ID"+i+">other</td></tr>");
				}
			}
		});
		
		String regex = "<tr>.*?<td>(.*?)</td>.*?<td id=(.*?)>(.*?)</td>.*?</tr>";

		RegexFileItemReader<TestItem> itemReader = createRegexFileItemReader(new FileSystemResource(f), regex);
		List<TestItem> result = readFromReader(itemReader);
		assertEquals(100, result.size());
		checkSequenceNumber(result);

	}

	@Test
	public void testFirstItemBetweenSecondAndThirdBlock() throws Exception {

		File f = createTestFile();

		FileWriterTemplate template = new FileWriterTemplate(f);
		template.write(new FileWriterAction() {

			@Override
			public void write(Writer writer) throws IOException {

				for(int i=0; i < 2*2048-4; i++) {
					writer.write("-");
				}

				for(int i=0; i < 100; i++) {
					writer.write("<tr><td>"+i+"</td><td id=ID"+i+">other</td></tr>");
				}
			}
		});
		
		String regex = "<tr>.*?<td>(.*?)</td>.*?<td id=(.*?)>(.*?)</td>.*?</tr>";

		RegexFileItemReader<TestItem> itemReader = createRegexFileItemReader(new FileSystemResource(f), regex);
		List<TestItem> result = readFromReader(itemReader);
		assertEquals(100, result.size());
		checkSequenceNumber(result);
	}

	@Test
	public void testFirstItemBetweenFirstAndSecondBlockAndSecondItemBetweenSecondAndThirdBlock() throws Exception {

		File f = createTestFile();

		FileWriterTemplate template = new FileWriterTemplate(f);
		template.write(new FileWriterAction() {

			@Override
			public void write(Writer writer) throws IOException {

				int seqNumber=0;
				
				for(int i=0; i < 2048-4; i++) {
					writer.write("-");
				}
				writer.write("<tr><td>"+Integer.toString(seqNumber++)+"</td><td id=IDX>other</td></tr>");

				for(int i=0; i < 2048-38; i++) {
					writer.write("-");
				}
				
				for(int i=0; i < 99; i++) {
					writer.write("<tr><td>"+Integer.toString(seqNumber++)+"</td><td id=ID"+i+">other</td></tr>");
				}
			}
		});
		
		String regex = "<tr>.*?<td>(.*?)</td>.*?<td id=(.*?)>(.*?)</td>.*?</tr>";

		RegexFileItemReader<TestItem> itemReader = createRegexFileItemReader(new FileSystemResource(f), regex);
		List<TestItem> result = readFromReader(itemReader);
		assertEquals(100, result.size());
		checkSequenceNumber(result);
	}

}
