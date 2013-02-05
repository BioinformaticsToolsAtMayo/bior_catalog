package edu.mayo.bior.utils;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import com.google.common.io.Files;

public class CatalogUtils {
	
	/** Use the JUnit asserts to validate if two files have the same size and content */
	public static void assertFileEquals(String fileExpected, String fileActual) throws IOException {
		List<String> linesExpected = Files.readLines(new File(fileExpected), Charset.forName("UTF-8"));
		List<String> linesActual   = Files.readLines(new File(fileActual), Charset.forName("UTF-8"));
		assertEquals("Not the same # of lines in each file.  ", linesExpected.size(), linesActual.size());
		for(int i = 0; i < Math.max(linesExpected.size(), linesActual.size()); i++) {
			assertEquals("Line " + (i+1) + " not equal: ", linesExpected.get(i), linesActual.get(i));
		}
		
		// Verify that the number of lines in the input equals the # of lines in the output catalog (one variant in to each variant out)
		assertEquals( "There should be the same number of variants in the output as were in the input.  ",
				Files.readLines(new File(fileExpected), Charset.forName("UTF-8")).size(),
				Files.readLines(new File(fileActual), Charset.forName("UTF-8")).size() );
	}

}
