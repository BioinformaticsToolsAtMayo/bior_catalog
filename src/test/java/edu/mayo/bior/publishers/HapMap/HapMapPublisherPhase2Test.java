package edu.mayo.bior.publishers.HapMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class HapMapPublisherPhase2Test {
	@Test
	public void testPhase2() throws IOException {
		
		String dir     = "src/test/resources/testData/hapmap/";
		String inFile  = dir + "hapmap.beforePhase2CollapsePopulations.tsv";
		String outFileTemp = dir + "hapMapPhase2.out.temp.tsv";
		HapMapPublisherPhase2 phase2 = new HapMapPublisherPhase2();
		phase2.publish(inFile, outFileTemp);
		
		String expectedFile = dir + "hapmap.expected.afterPhase2CollapsePopulations.tsv";
		assertFilesEqual(new File(expectedFile), new File(outFileTemp));
		
		// Delete the output file since we only needed it to compare to expected output
		new File(outFileTemp).delete();
	}

	private void assertFilesEqual(File expectedFile, File actualResultsFile) throws IOException {
		BufferedReader expected = new BufferedReader(new FileReader(expectedFile));
		BufferedReader actual = new BufferedReader(new FileReader(actualResultsFile));
		String lineExpected = "";
		int line = 1;
		while( (lineExpected = expected.readLine()) != null ) {
			String lineActual = actual.readLine();
			assertEquals("Line " + line + " different between expected and actual.", lineExpected, lineActual);
			line++;
		}
		assertEquals("File sizes not equal.  ", expectedFile.length(), actualResultsFile.length());
	}

}
