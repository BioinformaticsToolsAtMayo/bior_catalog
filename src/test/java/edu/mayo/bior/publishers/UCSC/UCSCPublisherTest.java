/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.UCSC;

import com.tinkerpop.pipes.Pipe;
import edu.mayo.pipes.JSON.DrillPipe;
import edu.mayo.pipes.JSON.InjectIntoJsonPipe;
import edu.mayo.pipes.JSON.inject.Injector;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.WritePipe;
import edu.mayo.pipes.history.HCutPipe;
import java.io.IOException;
import java.util.List;
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
public class UCSCPublisherTest {
    
    public UCSCPublisherTest() {
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
     * Test of usage method, of class UCSCPublisher.
     */
    @Test
    public void testPublishhiSeqDepthTop5Pct() throws IOException {
        System.out.println("testPublishhiSeqDepthTop5Pct");
        String testfile = "src/test/resources/testData/ucsc/vistaEnhancers.txt.gz";//do tests for all 7 columns in treat (8th requires liftover)
        String sql = "src/test/resources/testData/ucsc/vistaEnhancers.sql";
        UCSCPublisher ucsc = new UCSCPublisher();

        SQLParser sqlp = new SQLParser();        
        //System.out.println("Parsing SQL: " + indir + sql);
        List<String> lines = sqlp.loadFileToMemory(sql);
        for(int i=0; i<lines.size(); i++){
            //System.out.println(lines.get(i));//make an assert just on the important sql lines
        }
        System.out.println("Processing: " + testfile);
        Injector[] inj = sqlp.getInjectorsFromSQL(lines);
        InjectIntoJsonPipe inject = new InjectIntoJsonPipe(true, inj);
        HCutPipe cut = new HCutPipe(false, ucsc.cutArr(inj.length));
        DrillPipe drill = new DrillPipe(true, sqlp.getGoldenDrillPaths(lines, false));
        ucsc.process(testfile, inject, cut, drill, new PrintPipe());  //idea replace this print pipe with something else that will help us with testing...
        //things we have to test for each ucsc required by treat:
        //1. does the json have all of the information from the original data
        //2. does the json have the golden attrs: _minBP _maxBP _landmark
        //3. is the first 3 columns correct
        
    }


}
