package edu.mayo.bior.publishers.Cosmic;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.mayo.bior.utils.CatalogUtils;
import edu.mayo.bior.utils.GetBasesUtil;


public class CosmicPublisherTest {

	/**
	 * [[A1CF, ENST00000374001, 24086, Br27P, 1312984, 1223893, central_nervous_system, brain, glioma, astrocytoma_Grade_IV, n, 40879, c.976G>A, p.E326K, Substitution - Missense, het, 10:52245937-52245937, -, 10:52575931-52575931, -, Confirmed somatic variant, 18772396, surgery - NOS, secondary, Grade:Some Grade data are given in publication]]
	 */
	
    @Rule
    public TemporaryFolder tFolder = new TemporaryFolder();

    final String INPUT_TSV  	= "src/test/resources/testData/cosmic/cosmictest.tsv.gz";
    
    // instructions for building:
    // 1.) Exract relevant lines from full genome catalog:
    //     zcat cosmictest.tsv.gz | cut -f 19 | xargs tabix hs_ref_genome.fa.tsv.bgz > test_hs_ref_genome.fa.tsv
    // 2.) Build TABIX files
    //     sort -k 1,1 -k 2,2n -k 3,3n  test_hs_ref_genome.fa.tsv  |  bgzip >  test_hs_ref_genome.fa.tsv.bgz
    //     tabix -s 1 -b 2 -e 3 test_hs_ref_genome.fa.tsv.bgz 
    //
    final String INPUT_GENOME_CATALOG = "src/test/resources/testData/cosmic/test_hs_ref_genome.fa.tsv.bgz";
    
    final String EXPECTED_TSV= "src/test/resources/testData/cosmic/cosmic.expected.tsv";

    public File tempFolder;
    public File OUTPUT_TSV;

    @Before
    public void createTestData() throws IOException {
    	tempFolder = tFolder.newFolder("cosmicTempDir");
    	OUTPUT_TSV = new File(tempFolder, "cosmic_GRCh37.tsv");
    	
    	//System.out.println(tempFolder.getPath() +"::"+ output.getPath());
    }
	
    //@Test
    public void testgetBPatPos() throws IOException{
        GetBasesUtil baseu = new GetBasesUtil();
    	System.out.println("Testing getBPatPos... Make sure your sys.properties is set up to include hs_complete_genome_catalog and that catalog is on your path");
        CosmicPublisher cp = new CosmicPublisher(); 
        String bPatPos = baseu.getBasePairAtPosition("9", "133748282", "133748283");
        System.out.println(bPatPos);
    }
       
    @Test
    public void testTransform() throws Exception {
    	System.out.println("Testing.. CosmicPublisherTest.testTransform()!!");

        new CosmicPublisher().publish(INPUT_TSV, INPUT_GENOME_CATALOG, tempFolder);

        CatalogUtils.assertFileEquals(EXPECTED_TSV, OUTPUT_TSV.getPath());        
    }    
}
