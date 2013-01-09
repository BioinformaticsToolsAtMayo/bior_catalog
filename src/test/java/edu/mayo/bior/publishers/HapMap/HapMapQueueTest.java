/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.HapMap;

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
public class HapMapQueueTest {
    
    public HapMapQueueTest() {
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
     * Test of mergeHapMap method, of class HapMapQueue.
     */
    @Test
    public void testMergeHapMap() {
//        System.out.println("mergeHapMap");
//        String current = "";
//        String jsonVariant = "";
//        HapMapQueue instance = new HapMapQueue();
//        String expResult = "";
//        String result = instance.mergeHapMap(current, jsonVariant);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
    }

    /**
     * Test of constructFromOne method, of class HapMapQueue.
     */
    @Test
    public void testConstructFromOne() {
        System.out.println("constructFromOne");
        String jsonVariant = "{\"rsNumber\":\"rs10399749\",\"chrom\":\"chr1\",\"pos\":45162,\"strand\":\"+\",\"build\":\"ncbi_b36\",\"center\":\"perlegen\",\"protLSID\":\"urn:lsid:perlegen.hapmap.org:Protocol:Genotyping_1.0.0:2\",\"assayLSID\":\"urn:lsid:perlegen.hapmap.org:Assay:25761.5318498:1\",\"panelLSID\":\"urn:LSID:dcc.hapmap.org:Panel:Han_Chinese:2\",\"QC_code\":\"QC+\",\"refallele\":\"C\",\"refallele_freq\":1.0,\"refallele_count\":88,\"otherallele\":\"T\",\"otherallele_freq\":0,\"otherallele_count\":0,\"totalcount\":88,\"population\":\"CHB\", \"_type\":\"variant\", \"_landmark\":1, \"_minBP\":55299, \"_maxBP\":55299, \"_strand\":\"+\", \"_refAllele\":\"C\", \"_altAlleles\":\"T\", \"_id\":\"rs10399749\"}";
        HapMapQueue instance = new HapMapQueue();
        String expResult = "";
        String result = instance.constructFromOne(jsonVariant);
        System.out.println(result);
        //assertEquals(expResult, result);
        
    }
}
