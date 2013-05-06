package edu.mayo.bior.publishers.OMIM;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.mayo.bior.utils.CatalogUtils;

public class OMIMPublisherTest {

	@Rule
    public TemporaryFolder tFolder = new TemporaryFolder();

    //final String INPUT_TSV    = "src/test/resources/testData/omim/genemap";
    //final String EXPECTED_TSV = "src/test/resources/testData/omim/omim.expected.tsv";

    public File tempFolder;
    public File OUTPUT_TSV;
    public File OUTPUT_BGZ;
    
    @Before
    public void createTestData() throws IOException {
    	tempFolder = tFolder.newFolder("omimTmpDir");
    	System.out.println("Temp folder: " + tempFolder.getCanonicalPath());
    	OUTPUT_TSV = new File(tempFolder.getCanonicalPath() + "/scratch", "genemap_GRCh37.tsv");
    	OUTPUT_BGZ = new File(tempFolder, "genemap_GRCh37.tsv.bgz");
    }

    @Test
    public void testExec() throws IOException {
    	System.out.println("Testing OMIMPublisher.testExec()...");    	
    	new LoadGenes().exec("src/test/resources/testData/omim/genemap", tempFolder.getPath()+"/");
    	//System.out.println(FileCompareUtils.loadFile(OUTPUT_TSV.getCanonicalPath()));
    	CatalogUtils.assertFileEquals("src/test/resources/testData/omim/omim.expected.tsv", OUTPUT_TSV.getPath());
    	assertTrue(OUTPUT_BGZ.exists());
    	assertTrue(OUTPUT_BGZ.length() > 0);
    }
}
