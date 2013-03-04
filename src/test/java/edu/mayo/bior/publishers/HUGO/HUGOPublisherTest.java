package edu.mayo.bior.publishers.HUGO;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.jayway.jsonpath.JsonPath;

import edu.mayo.bior.utils.CatalogUtils;

public class HUGOPublisherTest {
	@Rule
    public TemporaryFolder tFolder = new TemporaryFolder();

    final String INPUT_TSV  	= "src/test/resources/testData/hugo/hgnc_sample_dataset.txt";
    final String EXPECTED_TSV= "src/test/resources/testData/hugo/hgnc.expected.tsv";

    public File tempFolder;
    public File OUTPUT_TSV;
    
    @Before
    public void createTestData() throws IOException {
    	tempFolder = tFolder.newFolder("hugoTmpDir");
    	OUTPUT_TSV = new File(tempFolder, "hgnc_GRCh37.tsv");
    }

    @Test
    public void testExec() throws IOException {
    	System.out.println("Testing HUGOPublisher.testExec()...");    	
    	HUGOPublisher publisher = new HUGOPublisher();    	
    	publisher.exec(INPUT_TSV, tempFolder.getPath()+"/");    	
    	CatalogUtils.assertFileEquals(EXPECTED_TSV, OUTPUT_TSV.getPath()); 
    }
    
    @Test
    public void testExecResults() throws IOException {
    	System.out.println("Testing HUGOPublisherTest.testExecResults()...");    	
    	
    	String EXPECTED_LINE = ".	0	0	{\"HGNC_ID\":\"HGNC:5\",\"Approved_Symbol\":\"A1BG\",\"Approved_Name\":\"alpha-1-B glycoprotein\",\"Status\":\"Approved\",\"Locus_Type\":\"gene with protein product\",\"Locus_Group\":\"protein-coding gene\",\"Previous_Symbols\":[],\"Previous_Names\":[],\"Synonyms\":[],\"Name_Synonyms\":[],\"Chromosome\":\"19q\",\"Date_Approved\":\"1989-06-30\",\"Date_Modified\":\"2010-07-08\",\"Accession_Numbers\":[],\"Enzyme_IDs\":[],\"Entrez_Gene_ID\":\"1\",\"Ensembl_Gene_ID\":\"ENSG00000121410\",\"Specialist_Database_Links\":\"<!--,--> <!--,--> <!--,--> <!--,--> <!--,--> <!--,--> <!--,--> <!--,--> <!--,--> <a href=\"http://merops.sanger.ac.uk/cgi-bin/merops.cgi?id=I43.950\">MEROPS</a><!--,--> <a href=\"http://www.sanger.ac.uk/perl/genetics/CGP/cosmic?action=gene&amp;ln=A1BG\">COSMIC</a><!--,--> <!--,--> <!--,--> <!--,--> <!--,--> <!--,--> \",\"Specialist_Database_IDs\":[\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"I43.950\",\"A1BG\",\"\",\"\",\"\",\"\",\"\",\"\"],\"Pubmed_IDs\":[\"2591067\"],\"RefSeq_IDs\":[\"NM_130786\"],\"Record_Type\":\"Standard\",\"Primary_IDs\":[],\"Secondary_IDs\":[],\"CCDS_IDs\":[\"CCDS12976.1\"],\"VEGA_IDs\":[],\"mapped_GDB_ID\":\"GDB:119638\",\"mapped_Entrez_Gene_ID\":\"1\",\"mapped_OMIM_ID\":\"138670\",\"mapped_RefSeq\":\"NM_130786\",\"UniProt_ID\":\"P04217\",\"mapped_Ensembl_ID\":\"ENSG00000121410\",\"UCSC_ID\":\"uc002qsd.4\",\"mapped_Mouse_Genome_Database_ID\":\"MGI:2152878\",\"mapped_Rat_Genome_Database_ID\":\"RGD:69417\"}";
    	
    	String INPUT  = "src/test/resources/testData/hugo/hgnc_sample_dataset.txt";
    	HUGOPublisher publisher = new HUGOPublisher();    	
    	publisher.exec(INPUT, tempFolder.getPath()+"/");    	
    	BufferedReader result = new BufferedReader(new FileReader(OUTPUT_TSV));    	

    	String actual = result.readLine();

    	String[] aSplit = actual.split("\t");
    	
    	String json = aSplit[3];
    	    	
    	assertEquals("HGNC:5", JsonPath.compile("HGNC_ID").read(json));
    	assertEquals("A1BG", JsonPath.compile("Approved_Symbol").read(json));
    }
}
