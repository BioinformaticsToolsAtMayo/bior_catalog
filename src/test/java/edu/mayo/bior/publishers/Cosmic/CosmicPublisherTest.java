package edu.mayo.bior.publishers.Cosmic;

import java.io.IOException;
import org.junit.Test;

public class CosmicPublisherTest {

	/**
	 * [[A1CF, ENST00000374001, 24086, Br27P, 1312984, 1223893, central_nervous_system, brain, glioma, astrocytoma_Grade_IV, n, 40879, c.976G>A, p.E326K, Substitution - Missense, het, 10:52245937-52245937, -, 10:52575931-52575931, -, Confirmed somatic variant, 18772396, surgery - NOS, secondary, Grade:Some Grade data are given in publication]]
	 */
	
//	@Test
//	public void testCosmicProcess() throws Exception {
//		String input = "A1CF, ENST00000374001, 24086, Br27P, 1312984, 1223893, central_nervous_system, brain, glioma, astrocytoma_Grade_IV, n, 40879, c.976G>A, p.E326K, Substitution - Missense, het, 10:52245937-52245937, -, 10:52575931-52575931, -, Confirmed somatic variant, 18772396, surgery - NOS, secondary, Grade:Some Grade data are given in publication";
//		
//		//CosmicPublisher cp = new CosmicPublisher();
//		//cp.publish(rawDataFile, outputDir)
//	}
    
    
    
       @Test
       public void testgetBPatPos() throws IOException{
           System.out.println("Testing getBPatPos... Make sure your sys.properties is set up to include hs_complete_genome_catalog and that catalog is on your path");
           CosmicPublisher cp = new CosmicPublisher(); 
           String bPatPos = cp.getBPatPos("9", "133748282", "133748283");
           System.out.println(bPatPos);
       }
}
