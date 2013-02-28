package edu.mayo.bior.publishers.OMIM;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.mayo.bior.utils.CatalogUtils;
import edu.mayo.pipes.WritePipe;

public class OMIMPublisherTest {

	@Rule
    public TemporaryFolder tFolder = new TemporaryFolder();

    final String INPUT_TSV  	= "src/test/resources/testData/hugo/genemap";
    final String EXPECTED_TSV= "src/test/resources/testData/hugo/genemap.expected.tsv";

    public File tempFolder;
    public File OUTPUT_TSV;
    public File OUTPUT_TSV2;
    
    @Before
    public void createTestData() throws IOException {
    	tempFolder = tFolder.newFolder("hugoTmpDir");
    	OUTPUT_TSV = new File(tempFolder, "genemap.tsv");
    	OUTPUT_TSV2 = new File(tempFolder, "genemap2.tsv");
    }

    @Test
    public void testExec() throws IOException {
    	System.out.println("Testing OMIMPublisher.testExec()...");    	
    	WritePipe writePipe = new WritePipe(OUTPUT_TSV.getPath());    	
    	LoadGenes loadGenes = new LoadGenes();    	
    	loadGenes.exec(INPUT_TSV, writePipe);    	
    	CatalogUtils.assertFileEquals(EXPECTED_TSV, OUTPUT_TSV.getPath()); 
    }
    
    @Test
    public void testExecResults() throws IOException {
    	System.out.println("Testing OMIMPublisherTest.testExecResults()...");    	
    	
    	String EXPECTED_LINE = ".	.	.	{\"Chromosome.Map_Entry_Number\":1.1,\"MonthEntered\":9,\"Day\":11,\"Year\":95,\"Cytogenetic_location\":\"1pter-p36.13\",\"GeneSymbols\":\"CCV\",\"Gene_Status\":\"P\",\"Title\":\"Cataract, congenital, Volkmann type\",\"Title_cont\":\"\",\"MIM_Number\":115665,\"Method\":\"Fd\",\"Comments\":\"\",\"Disorders\":\"Cataract, congenital, Volkmann type (2)\",\"Disorders_cont\":\" \"}";

    	String INPUT  = "src/test/resources/testData/hugo/genemap_sample";
    	//File OUTPUT = new File("src/test/resources/testData/hugo/genemap_sample.tsv");
    	
    	WritePipe writePipe = new WritePipe(OUTPUT_TSV2.getPath());    	    	
    	LoadGenes loadGenes = new LoadGenes();    	
    	loadGenes.exec(INPUT, writePipe);    	
    	BufferedReader result = new BufferedReader(new FileReader(OUTPUT_TSV2));    	
    	String actual = result.readLine();

    	assertEquals(EXPECTED_LINE, actual);
    }
    
}
