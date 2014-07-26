package org.springframework.batch.item.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.RegexLineTokenizer;
import org.springframework.core.io.FileSystemResource;

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
	
	private RegexFileItemReader<TestItem> createRegexFileItemReader(final File file, final String regex) throws Exception {
		
		RegexFileItemReader<TestItem> reader = new RegexFileItemReader<TestItem>();
		reader.setLineMapper(getLineMapperForRegex(regex));
		reader.setPattern(Pattern.compile(regex, Pattern.DOTALL));
		reader.setResource(new FileSystemResource(file));
		reader.afterPropertiesSet();
		
		return reader;
	}
	
	private List<TestItem> readFromReader(final RegexFileItemReader<TestItem> itemReader) throws Exception {
		
		List<TestItem> result = new ArrayList<TestItem>();
		TestItem item;
		
		itemReader.doOpen();
		while((item = itemReader.doRead()) != null) {
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
		
		RegexFileItemReader<TestItem> itemReader = createRegexFileItemReader(f, regex);
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
		
		RegexFileItemReader<TestItem> itemReader = createRegexFileItemReader(f, regex);
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
		
		RegexFileItemReader<TestItem> itemReader = createRegexFileItemReader(f, regex);
		List<TestItem> result = readFromReader(itemReader);
		assertEquals(0, result.size());
	}

}
