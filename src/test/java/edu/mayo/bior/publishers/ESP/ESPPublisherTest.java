package edu.mayo.bior.publishers.ESP;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.mayo.bior.utils.CatalogUtils;

public class ESPPublisherTest {
	@Rule
    public TemporaryFolder tFolder = new TemporaryFolder();

    final String INPUT_TSV  	= "src/test/resources/testData/esp";
    final String EXPECTED_TSV= "src/test/resources/testData/esp/ESP6500SI_GRCh37.expected.tsv";

    public File tempFolder;
    public File OUTPUT_TSV;

    @Before
    public void createTestData() throws IOException {
    	tempFolder = tFolder.newFolder("espTempDir");
    	OUTPUT_TSV = new File(tempFolder, "ESP6500SI_GRCh37.expected.tsv");
    }
    
    @Test
    public void testPublish() throws Exception {
    	System.out.println("Testing.. ESPPublisherTest.testPublish()!!");

        new ESPPublisher().publish(INPUT_TSV, tempFolder.getPath()+"/ESP6500SI_GRCh37.expected.tsv");
    	
        CatalogUtils.assertFileEquals(EXPECTED_TSV, OUTPUT_TSV.getPath());        
    }    
}
