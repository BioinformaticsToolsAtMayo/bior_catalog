package edu.mayo.bior.publishers.Cosmic;

import java.io.IOException;

import org.junit.Test;

import edu.mayo.bior.utils.CatalogUtils;


public class CosmicPublisherTest {

	/**
	 * [[A1CF, ENST00000374001, 24086, Br27P, 1312984, 1223893, central_nervous_system, brain, glioma, astrocytoma_Grade_IV, n, 40879, c.976G>A, p.E326K, Substitution - Missense, het, 10:52245937-52245937, -, 10:52575931-52575931, -, Confirmed somatic variant, 18772396, surgery - NOS, secondary, Grade:Some Grade data are given in publication]]
	 */
   
       //@Test
       public void testgetBPatPos() throws IOException{
           System.out.println("Testing getBPatPos... Make sure your sys.properties is set up to include hs_complete_genome_catalog and that catalog is on your path");
           CosmicPublisher cp = new CosmicPublisher(); 
           String bPatPos = cp.getBasePairAtPosition("9", "133748282", "133748283");
           System.out.println(bPatPos);
       }
       
       @Test
       public void testTransform() throws Exception {
    	   System.out.println("Testing.. CosmicPublisherTest.testTransform()!!");

           final String INPUT_TSV  	= "src/test/resources/testData/cosmic/cosmictest.tsv.gz";
           final String EXPECTED_TSV= "src/test/resources/testData/cosmic/cosmic.expected.tsv";
           final String OUTPUT_DIR	= "src/test/resources/testData/cosmic/tmpOut/";
           final String OUTPUT_TSV 	= "src/test/resources/testData/cosmic/tmpOut/cosmic_GRCh37.tsv";

           new CosmicPublisher().publish(INPUT_TSV, OUTPUT_DIR);

           CatalogUtils.assertFileEquals(EXPECTED_TSV, OUTPUT_TSV);
       }
}
