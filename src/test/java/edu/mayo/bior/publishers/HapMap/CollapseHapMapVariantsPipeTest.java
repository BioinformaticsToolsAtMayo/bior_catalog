package edu.mayo.bior.publishers.HapMap;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.UNIX.GrepPipe;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;

/**
 * @author Michael Meiners, Patrick Duffy
 */
public class CollapseHapMapVariantsPipeTest {
    
	private Gson mGson = new Gson();
	private JsonParser mParser = new JsonParser();
	
	private Pipe<String, History> mPipeline;
	
	// Stored the non-merged rows, 1 row per population
	// key    = rsID
	// value  = List of 1 or more rows
	private Map<String, List<String>> mInputVariants = new HashMap<String, List<String>>();

	// Stores the merged JSON, not the entire row
	// key    = rsID
	// value  = merged JSON String
	private Map<String, String> mMergedVariantsOut = new HashMap<String, String>();	

    @Before
    public void setUp() throws JsonIOException, JsonSyntaxException, IOException {
    	
    	File dataFolder = new File("src/test/resources/testData/hapmap");    	
    	
    	// rows from this file are extracted and stored in Lists by rsID
    	File dataFile = new File(dataFolder, "hapmap.sorted.liftover.first300.tsv");     	
    	mInputVariants.put("rs10399749", getLinesByStringMatch(dataFile, "rs10399749"));
    	mInputVariants.put("rs2691310",  getLinesByStringMatch(dataFile, "rs2691310"));
    	mInputVariants.put("rs2949420",  getLinesByStringMatch(dataFile, "rs2949420"));
    	mInputVariants.put("rs2949421",  getLinesByStringMatch(dataFile, "rs2949421"));
    	
    	// load the merged data for each variant
    	mMergedVariantsOut.put("rs10399749", loadJsonDocument(new File(dataFolder, "rs10399749_merged.json")));
    	mMergedVariantsOut.put("rs2691310",  loadJsonDocument(new File(dataFolder, "rs2691310_merged.json")));
    	mMergedVariantsOut.put("rs2949420",  loadJsonDocument(new File(dataFolder, "rs2949420_merged.json")));
    	mMergedVariantsOut.put("rs2949421",  loadJsonDocument(new File(dataFolder, "rs2949421_merged.json")));
    	
    	// pipes
    	HistoryInPipe 				historyIn	= new HistoryInPipe();
    	CollapseHapMapVariantsPipe	logic		= new CollapseHapMapVariantsPipe();
    	
		// pipeline definition
		mPipeline = new Pipeline<String, History>
			(
					historyIn,	// String			--> history
					logic		// history			--> modified history*
			);    	
    }
    
    @After
    public void tearDown() {
    	mPipeline.reset();
    	mInputVariants.clear();
    	mMergedVariantsOut.clear();
    }
    
    @Test
    public void testNoVariants() {
    	test(new String[0]);
    }
    
    @Test
    public void testSingleVariantOnly1() {
    	// pass in 1 variant that has only 1 population
    	test("rs2691310");
    }
    
    @Test
    public void testSingleVariantOnly2() {
    	// pass in 1 variant that has multiple populations
        test("rs2949420");
    }    
    
    @Test
    public void testMultiplePopulationVariants1() {
    	test( 	"rs10399749", // has 4 population variants
        		"rs2691310",  // has 1 population variant 
        		"rs2949420"   // has 2 population variants
    	);
    }
    
    @Test
    public void testMultiplePopulationVariants2() {
        test(   "rs10399749", // has 4 population variants
        		"rs2691310",  // has 1 population variant 
        		"rs2949420",  // has 2 population variants
        		"rs2949421"   // has 1 population variants
        );    	
    }    
    
    /**
     * Helper method that processes the given variants through the test pipeline.
     * 
     * @param variantIDs
     */
    private void test(String... variantIDs) {
    	// build up the lines of non-merged variant data
    	List<String> inputLines = new ArrayList<String>();    	
    	for (String rsID: variantIDs) {
        	inputLines.addAll(mInputVariants.get(rsID));    		
    	}
    	    	
    	// Reset pipe
    	mPipeline.reset();
		// prime pipeline
        mPipeline.setStarts(inputLines);
        
        // run pipeline, grab row by row
    	for (String rsID: variantIDs) {
            mPipeline.hasNext();
            History history = mPipeline.next();
            String json = getCompactJSON(history.get(history.size() - 1));
//            String json = history.get(history.size() - 1);
            
            String expectedJSON = mMergedVariantsOut.get(rsID); 
            
            System.out.println("====");
            System.out.println("Expected: " + expectedJSON);
            System.out.println("Actual:   " + json);
            
            assertEquals(expectedJSON, json);    		
    	}
    }
    
    /**
     * Gets lines from the given file that match the specified string.
     * 
     * @param dataFile The file to be searched.
     * @param literalStr The literal string to search for on each file line.
     * @return
     */
    private List<String> getLinesByStringMatch(File dataFile, String literalStr) {
    	
    	List<String> l = new ArrayList<String>();
    	
    	CatPipe cat = new CatPipe();
    	GrepPipe grep = new GrepPipe(".+"+literalStr+".+");
    	
    	Pipeline<String, String> p = new Pipeline<String, String>(cat, grep);
    	p.setStarts(Arrays.asList(dataFile.getAbsolutePath()));
    	
    	while (p.hasNext()) {
    		String line = p.next();
    		l.add(line);
    	}
    	return l;
    }
    
    /**
     * Loads a JSON object from a file into a compact string (e.g. no extra whitespace or linefeeds).
     * 
     * @param jsonFile
     * @return
     * @throws JsonIOException
     * @throws JsonSyntaxException
     * @throws FileNotFoundException
     */
    private String loadJsonDocument(File jsonFile) throws JsonIOException, JsonSyntaxException, FileNotFoundException {
    	JsonElement el = mParser.parse(new FileReader(jsonFile));
    	return mGson.toJson(el);
    }
    
    /**
     * Compacts the JSON into a one-liner
     * @param json
     * @return
     */
    private String getCompactJSON(String json) {
    	JsonElement el = mParser.parse(json);
    	return mGson.toJson(el);    	
    }
        
}
