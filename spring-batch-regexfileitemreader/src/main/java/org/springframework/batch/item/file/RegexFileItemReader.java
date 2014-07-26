package org.springframework.batch.item.file;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ReaderNotOpenException;
import org.springframework.batch.item.file.BufferedReaderFactory;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.NonTransientFlatFileException;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

public class RegexFileItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements
ResourceAwareItemReaderItemStream<T>, InitializingBean {

	private static Log logger = LogFactory.getLog(RegexFileItemReader.class);

	// default encoding for input files
	public static String DEFAULT_CHARSET = Charset.defaultCharset().name();
	
	private Resource resource;

	private BufferedReader reader;
	
	private String encoding = DEFAULT_CHARSET;

	private LineMapper<T> lineMapper;
	
	private int lineCount = 0;
	
	private boolean noInput = false;
	
	private Pattern pattern = null;
	
	private boolean strict = true;
	
	private BufferedReaderFactory bufferedReaderFactory = new DefaultBufferedReaderFactory();

	public RegexFileItemReader() {
		setName(ClassUtils.getShortName(RegexFileItemReader.class));
	}
	
	/**
	 * In strict mode the reader will throw an exception on
	 * {@link #open(org.springframework.batch.item.ExecutionContext)} if the input resource does not exist.
	 * @param strict <code>true</code> by default
	 */
	public void setStrict(boolean strict) {
		this.strict = strict;
	}

	/**
	 * Setter for line mapper. This property is required to be set.
	 * @param lineMapper maps line to item
	 */
	public void setLineMapper(LineMapper<T> lineMapper) {
		this.lineMapper = lineMapper;
	}
	
	/**
	 * Setter for the encoding for this input source. Default value is {@link #DEFAULT_CHARSET}.
	 * 
	 * @param encoding a properties object which possibly contains the encoding for this input file;
	 */
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}
	
	/**
	 * Factory for the {@link BufferedReader} that will be used to extract lines from the file. The default is fine for
	 * plain text files, but this is a useful strategy for binary files where the standard BufferedReaader from java.io
	 * is limiting.
	 * 
	 * @param bufferedReaderFactory the bufferedReaderFactory to set
	 */
	public void setBufferedReaderFactory(BufferedReaderFactory bufferedReaderFactory) {
		this.bufferedReaderFactory = bufferedReaderFactory;
	}
	
	public void setPattern(Pattern pattern) {
		this.pattern = pattern;
	}
	
	/**
	 * Public setter for the input resource.
	 */
    @Override
	public void setResource(Resource resource) {
		this.resource = resource;
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(lineMapper, "LineMapper is required");		
		Assert.notNull(pattern, "Pattern is required");		
	}


	@Override
	protected T doRead() throws Exception {
		if (noInput) {
			return null;
		}

		String line = readLine();

		if (line == null) {
			return null;
		}
		else {
			try {
				return lineMapper.mapLine(line, lineCount);
			}
			catch (Exception ex) {
				throw new FlatFileParseException("Parsing error at line: " + lineCount + " in resource=["
						+ resource.getDescription() + "], input=[" + line + "]", ex, line, lineCount);
			}
		}
	}

	private int bufferSize = 2048;
	private int currentBufferSize = 2048;
	private int offsetBuffer = 0;
	private int offsetLastStart = 0;
	private int offsetLastEnd = 0;
	private char[] buffer = new char[2*bufferSize];
	private boolean readBufferNeeded = true;
	
	private boolean readBuffer() throws IOException {
		
		if(!readBufferNeeded) {
			return true;
		}
		
		readBufferNeeded = false;
		int read = this.reader.read(buffer, offsetBuffer, bufferSize);
		if(read==-1) {
			return false;
		}
		
		return true;
	}
	
	private void copy(char[] b, int from, int len, int to) {
		
		for(int idx = from; idx < from + len; idx++) {
			b[to++] = b[idx];
		}
		
	}

	/**
	 * @return next line (skip comments).getCurrentResource
	 */
	private String readLine() {

		if (reader == null) {
			throw new ReaderNotOpenException("Reader must be open before it can be read.");
		}

		String line = null;

		try {
			

			while(1==1) {
			
				boolean readed = readBuffer();
				if(!readed) {
					return null;
				}
				
				String bufferString = String.valueOf(buffer);
				Matcher matcher = pattern.matcher(bufferString);
				boolean found = matcher.find(offsetLastEnd);
				if(found) {
					offsetLastStart = matcher.start();
					offsetLastEnd = matcher.end();
					line = bufferString.substring(offsetLastStart, offsetLastEnd);
					lineCount++;
					break;
					
				} else {

					readBufferNeeded = true;
					
					if(offsetLastEnd!=0) {
						int restLen = currentBufferSize-offsetLastEnd;
						// move to beggining of buffer chunk after last item found
						copy(buffer, offsetLastEnd, currentBufferSize-offsetLastEnd, 0);
						Arrays.fill(buffer, restLen, buffer.length, (char)0);
						
						// new buffer's length will contain this chunk
						currentBufferSize = bufferSize + restLen;
						offsetBuffer = restLen;
						offsetLastEnd = 0;
						
					} else {
						// if can't find any item in current buffer then read only next buffer
						// TODO: what if first item starts in first buffer and is in first and second buffer? 
						offsetBuffer = 0;
						currentBufferSize = bufferSize;
					}
				}
			}
			
			if (line == null) {
				return null;
			}

		}
		catch (IOException e) {
			// Prevent IOException from recurring indefinitely
			// if client keeps catching and re-calling
			noInput = true;
			throw new NonTransientFlatFileException("Unable to read from resource: [" + resource + "]", e, line,
					lineCount);
		}
		return line;
	}	
	
	@Override
	protected void doOpen() throws Exception {
		Assert.notNull(resource, "Input resource must be set");

		noInput = true;
		if (!resource.exists()) {
			if (strict) {
				throw new IllegalStateException("Input resource must exist (reader is in 'strict' mode): " + resource);
			}
			logger.warn("Input resource does not exist " + resource.getDescription());
			return;
		}

		if (!resource.isReadable()) {
			if (strict) {
				throw new IllegalStateException("Input resource must be readable (reader is in 'strict' mode): "
						+ resource);
			}
			logger.warn("Input resource is not readable " + resource.getDescription());
			return;
		}

		reader = bufferedReaderFactory.create(resource, encoding);

		noInput = false;
	}

	@Override
	protected void doClose() throws Exception {
		lineCount = 0;
		offsetBuffer = 0;
		offsetLastStart = 0;
		offsetLastEnd = 0;
		readBufferNeeded = true;		
		
		if (reader != null) {
			reader.close();
		}		
	}

}
