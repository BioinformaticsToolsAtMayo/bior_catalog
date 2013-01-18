package edu.mayo.bior.publishers.HapMap;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/** @author Daniel Quest, Michael Meiners  */
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

    private final String jsonVariant  = "{\"rsNumber\":\"rs10399749\",\"chrom\":\"chr1\",\"pos\":45162,\"strand\":\"+\",\"build\":\"ncbi_b36\",\"center\":\"perlegen\",\"protLSID\":\"urn:lsid:perlegen.hapmap.org:Protocol:Genotyping_1.0.0:2\",\"assayLSID\":\"urn:lsid:perlegen.hapmap.org:Assay:25761.5318498:1\",\"panelLSID\":\"urn:LSID:dcc.hapmap.org:Panel:Han_Chinese:2\",\"QC_code\":\"QC+\",\"refallele\":\"C\",\"refallele_freq\":1.0,\"refallele_count\":88,\"otherallele\":\"T\",\"otherallele_freq\":0,\"otherallele_count\":0,\"totalcount\":88,\"population\":\"CHB\", \"_type\":\"variant\", \"_landmark\":\"1\", \"_minBP\":55299, \"_maxBP\":55299, \"_strand\":\"+\", \"_refAllele\":\"C\", \"_altAlleles\":\"T\", \"_id\":\"rs10399749\"}";
    private final String jsonVariant2 = "{\"rsNumber\":\"rs10399749\",\"chrom\":\"chr1\",\"pos\":45162,\"strand\":\"+\",\"build\":\"ncbi_b36\",\"center\":\"perlegen\",\"protLSID\":\"urn:lsid:perlegen.hapmap.org:Protocol:Genotyping_1.0.0:2\",\"assayLSID\":\"urn:lsid:perlegen.hapmap.org:Assay:25761.5318498:1\",\"panelLSID\":\"urn:LSID:dcc.hapmap.org:Panel:Japanese:2\",\"QC_code\":\"QC+\",\"refallele\":\"C\",\"refallele_freq\":1.0,\"refallele_count\":88,\"otherallele\":\"T\",\"otherallele_freq\":0,\"otherallele_count\":0,\"totalcount\":88,\"population\":\"JPT\", \"_type\":\"variant\", \"_landmark\":\"1\", \"_minBP\":55299, \"_maxBP\":55299, \"_strand\":\"+\", \"_refAllele\":\"C\", \"_altAlleles\":\"T\", \"_id\":\"rs10399749\"}";
    private final String jsonVariant3 = "{\"rsNumber\":\"rs10399749\",\"chrom\":\"chr1\",\"pos\":45162,\"strand\":\"+\",\"build\":\"ncbi_b36\",\"center\":\"perlegen\",\"protLSID\":\"urn:lsid:perlegen.hapmap.org:Protocol:Genotyping_1.0.0:2\",\"assayLSID\":\"urn:lsid:perlegen.hapmap.org:Assay:25761.5318498:1\",\"panelLSID\":\"urn:LSID:dcc.hapmap.org:Panel:Yoruba-30-trios:1\",\"QC_code\":\"QC+\",\"refallele\":\"C\",\"refallele_freq\":1.0,\"refallele_count\":118,\"otherallele\":\"T\",\"otherallele_freq\":0,\"otherallele_count\":0,\"totalcount\":118,\"population\":\"YRI\", \"_type\":\"variant\", \"_landmark\":\"1\", \"_minBP\":55299, \"_maxBP\":55299, \"_strand\":\"+\", \"_refAllele\":\"C\", \"_altAlleles\":\"T\", \"_id\":\"rs10399749\"}";
    private final String jsonVariant4 = "{\"rsNumber\":\"rs2949420\",\"chrom\":\"chr1\",\"pos\":45257,\"strand\":\"+\",\"build\":\"ncbi_b36\",\"center\":\"sanger\",\"protLSID\":\"urn:lsid:illumina.hapmap.org:Protocol:Golden_Gate_1.0.0:1\",\"assayLSID\":\"urn:lsid:sanger.hapmap.org:Assay:4499502:1\",\"panelLSID\":\"urn:LSID:dcc.hapmap.org:Panel:Japanese:1\",\"QC_code\":\"QC+\",\"refallele\":\"T\",\"refallele_freq\":1.0,\"refallele_count\":88,\"otherallele\":\"A\",\"otherallele_freq\":0,\"otherallele_count\":0,\"totalcount\":88,\"population\":\"JPT\", \"_type\":\"variant\", \"_landmark\":\"1\", \"_minBP\":55394, \"_maxBP\":55394, \"_strand\":\"+\", \"_refAllele\":\"T\", \"_altAlleles\":\"A\", \"_id\":\"rs2949420\"}";
    private final String jsonVariant5 = "{\"rsNumber\":\"rs2949421\",\"chrom\":\"chr1\",\"pos\":45413,\"strand\":\"+\",\"build\":\"ncbi_b36\",\"center\":\"sanger\",\"protLSID\":\"urn:lsid:illumina.hapmap.org:Protocol:Golden_Gate_1.0.0:1\",\"assayLSID\":\"urn:lsid:sanger.hapmap.org:Assay:4322523:1\",\"panelLSID\":\"urn:LSID:dcc.hapmap.org:Panel:Yoruba-30-trios:1\",\"QC_code\":\"QC+\",\"refallele\":\"A\",\"refallele_freq\":0.032,\"refallele_count\":4,\"otherallele\":\"T\",\"otherallele_freq\":0.968,\"otherallele_count\":120,\"totalcount\":124,\"population\":\"YRI\", \"_type\":\"variant\", \"_landmark\":\"1\", \"_minBP\":55550, \"_maxBP\":55550, \"_strand\":\"+\", \"_refAllele\":\"A\", \"_altAlleles\":\"T\", \"_id\":\"rs2949421\"}";

    private final String jsonSimple  = "{\"_landmark\":\"chr1\",\"_refAllele\":\"C\",\"A\":1,\"B\":2,\"population\":\"Rochestefarian\",\"C\":3}";

    @Test
    /** Test basic functionality of rolling population data into a sub-JSON object */
    public void testBasicRollup() {
    	HapMapQueue q = new HapMapQueue();
    	String expected = "{\"_landmark\":\"chr1\",\"_refAllele\":\"C\",\"Rochestefarian\":{\"A\":1,\"B\":2,\"C\":3}}";
    	String actual   = q.collapsePopulation(jsonSimple);
    	assertEquals(expected, actual);
    }
    
    /**
     * Test of mergeHapMap method, of class HapMapQueue.
     */
    @Test
    public void testMergeHapMap() {
        HapMapQueue hmq = new HapMapQueue();
        String line1 = hmq.collapsePopulation(jsonVariant);
        String line2 = hmq.collapsePopulation(jsonVariant2);
        String expected = "{\"rsNumber\":\"rs10399749\",\"chrom\":\"chr1\",\"pos\":45162,\"strand\":\"+\",\"build\":\"ncbi_b36\",\"refallele\":\"C\",\"otherallele\":\"T\","
        		+ "\"_type\":\"variant\",\"_landmark\":\"1\",\"_minBP\":55299,\"_maxBP\":55299,\"_strand\":\"+\",\"_refAllele\":\"C\",\"_altAlleles\":\"T\",\"_id\":\"rs10399749\","
        		+ "\"CHB\":{\"center\":\"perlegen\",\"protLSID\":\"urn:lsid:perlegen.hapmap.org:Protocol:Genotyping_1.0.0:2\",\"assayLSID\":\"urn:lsid:perlegen.hapmap.org:Assay:25761.5318498:1\",\"panelLSID\":\"urn:LSID:dcc.hapmap.org:Panel:Han_Chinese:2\",\"QC_code\":\"QC+\",\"refallele_freq\":1.0,\"refallele_count\":88,\"otherallele_freq\":0,\"otherallele_count\":0,\"totalcount\":88},"
        		+ "\"JPT\":{\"center\":\"perlegen\",\"protLSID\":\"urn:lsid:perlegen.hapmap.org:Protocol:Genotyping_1.0.0:2\",\"assayLSID\":\"urn:lsid:perlegen.hapmap.org:Assay:25761.5318498:1\",\"panelLSID\":\"urn:LSID:dcc.hapmap.org:Panel:Japanese:2\",\"QC_code\":\"QC+\",\"refallele_freq\":1.0,\"refallele_count\":88,\"otherallele_freq\":0,\"otherallele_count\":0,\"totalcount\":88}}"; 
        String actual = hmq.mergeHapMap(line1,line2);
        System.out.println("Expected: " + expected);
        System.out.println("Actual:   " + actual);
        assertEquals(expected, actual);
    }

    /**
     * Test of constructFromOne method, of class HapMapQueue.
     */
    @Test
    public void testConstructFromOne() {
        HapMapQueue instance = new HapMapQueue();
        String expResult = "{\"rsNumber\":\"rs10399749\",\"chrom\":\"chr1\",\"pos\":45162,\"strand\":\"+\",\"build\":\"ncbi_b36\",\"refallele\":\"C\",\"otherallele\":\"T\",\"_type\":\"variant\",\"_landmark\":\"1\",\"_minBP\":55299,\"_maxBP\":55299,\"_strand\":\"+\",\"_refAllele\":\"C\",\"_altAlleles\":\"T\",\"_id\":\"rs10399749\",\"CHB\":{"
        		+ "\"center\":\"perlegen\",\"protLSID\":\"urn:lsid:perlegen.hapmap.org:Protocol:Genotyping_1.0.0:2\",\"assayLSID\":\"urn:lsid:perlegen.hapmap.org:Assay:25761.5318498:1\",\"panelLSID\":\"urn:LSID:dcc.hapmap.org:Panel:Han_Chinese:2\",\"QC_code\":\"QC+\",\"refallele_freq\":1.0,\"refallele_count\":88,\"otherallele_freq\":0,\"otherallele_count\":0,\"totalcount\":88}}";
        String result = instance.collapsePopulation(jsonVariant);
        assertEquals(expResult, result);
    }
    
    @Test
    public void testConstructFromOneSimple() {
        HapMapQueue instance = new HapMapQueue();
        String expResult = "{\"_landmark\":\"chr1\",\"_refAllele\":\"C\",\"Rochestefarian\":{\"A\":1,\"B\":2,\"C\":3}}";
        String result = instance.collapsePopulation(jsonSimple);
        assertEquals(expResult, result);
    }
    
    @Test
    public void addElegantTest(){
        HapMapQueue hmq = new HapMapQueue();
        JsonObject none = new JsonObject(); 
        assertEquals("{}", none.toString());
        
        JsonObject i = new JsonParser().parse("{\"int1\": 33}").getAsJsonObject();
        JsonElement ji = i.get("int1");
        JsonObject withInt = hmq.addElegant(none, "int1", ji);
        assertEquals("{\"int1\":33}", withInt.toString());
        
        //test strings and landmarks
        none = new JsonObject();
        JsonObject landmark = new JsonParser().parse("{\""+CoreAttributes._landmark.toString()+"\":\"22\"}").getAsJsonObject();
        JsonElement jlandmark = landmark.get(CoreAttributes._landmark.toString());
        JsonObject withlandmark = hmq.addElegant(none, CoreAttributes._landmark.toString(), jlandmark);
        assertEquals("{\""+CoreAttributes._landmark.toString()+"\":\"22\"}", withlandmark.toString());
        
        //test doubles
        none = new JsonObject();
        JsonObject d = new JsonParser().parse("{\"d\": 22.1}").getAsJsonObject();
        JsonElement jd = d.get("d");
        JsonObject withDouble = hmq.addElegant(none, "d", jd);
        assertEquals("{\"d\":22.1}", withDouble.toString());

        // Test embedded json objects 
        none = new JsonObject();
        JsonObject jasonObj = new JsonParser().parse("{\"embedded\":{\"key\":\"value\"}}").getAsJsonObject();
        JsonElement jasonElem = jasonObj.get("embedded");
        JsonObject newJsonObj = hmq.addElegant(none, "embedded", jasonElem);
        assertEquals("{\"embedded\":{\"key\":\"value\"}}", newJsonObj.toString());
    }
}
