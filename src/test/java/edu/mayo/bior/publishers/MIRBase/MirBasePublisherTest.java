/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.MIRBase;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.transform.IdentityPipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.PrintPipe;
import java.util.Arrays;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class MirBasePublisherTest {
    
    public MirBasePublisherTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    
    
    /**
     * Test of getPipeline method, of class MirBasePublisher.
     */
    @Test
    public void testGetPipeline() {
        System.out.println("getPipeline");
        Pipe outPipe = new IdentityPipe();
        MirBasePublisher instance = new MirBasePublisher();
        String infile = "src/test/resources/testData/mirbase/hsa.gff2";
        String expResult = "1	30366	30503	{\"_landmark\":\"1\",\"type\":\"miRNA\",\"_minBP\":\"30366\",\"_maxBP\":\"30503\",\"_strand\":\"+\",\"ACC\":\"MI0006363\",\"ID\":\"hsa-mir-1302-2\"}";
        Pipeline p = instance.getPipeline(outPipe);
        p.setStarts(Arrays.asList(infile));
        for(int i=0; p.hasNext(); i++){
            String s = (String) p.next();
            if(i==0){
                assertEquals(s, expResult);
            }
        }
        
    }


}
