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
	private Map<String, List<String>> mVariantData = new HashMap<String, List<String>>();

	// Stores the merged JSON, not the entire row
	// key    = rsID
	// value  = merged JSON String
	private Map<String, String> mVariantMergedData = new HashMap<String, String>();	

    @Before
    public void setUp() throws JsonIOException, JsonSyntaxException, IOException {
    	
    	File dataFolder = new File("src/test/resources/testData/hapmap");    	
    	
    	// rows from this file are extracted and stored in Lists by rsID
    	File dataFile = new File(dataFolder, "hapmap.sorted.liftover.first300.tsv");     	
    	mVariantData.put("rs10399749", getLinesByStringMatch(dataFile, "rs10399749"));
    	mVariantData.put("rs2691310",  getLinesByStringMatch(dataFile, "rs2691310"));
    	mVariantData.put("rs2949420",  getLinesByStringMatch(dataFile, "rs2949420"));
    	mVariantData.put("rs2949421",  getLinesByStringMatch(dataFile, "rs2949421"));
    	
    	// load the merged data for each variant
    	mVariantMergedData.put("rs10399749", loadJsonDocument(new File(dataFolder, "rs10399749_merged.json")));
    	mVariantMergedData.put("rs2691310",  loadJsonDocument(new File(dataFolder, "rs2691310_merged.json")));
    	mVariantMergedData.put("rs2949420",  loadJsonDocument(new File(dataFolder, "rs2949420_merged.json")));
    	mVariantMergedData.put("rs2949421",  loadJsonDocument(new File(dataFolder, "rs2949421_merged.json")));
    	
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
    	mVariantData.clear();
    	mVariantMergedData.clear();
    }
    
    @Test
    public void testNoVariants() {
    	String[] variantIDs = new String[] {};
    	test(variantIDs);
    }
    
    @Test
    public void testSingleVariantOnly() {

    	String[] variantIDs;
    	
    	// pass in 1 variant that has only 1 population
    	variantIDs = new String[] {
    		"rs2691310"  // has 1 population variant 
    	};    	
    	test(variantIDs);

    	// pass in 1 variant that has multiple populations
    	variantIDs = new String[] {
        	"rs2949420",  // has 2 population variants
        };        	
        test(variantIDs);
    }    
    
    @Test
    public void testMultiplePopulationVariants() {
    	
    	String[] variantIDs;
    	
    	variantIDs = new String[] {
    		"rs10399749", // has 4 population variants
    		"rs2691310",  // has 1 population variant 
    		"rs2949420",  // has 2 population variants
    	};    	
    	test(variantIDs);
    	
    	variantIDs = new String[] {
        		"rs10399749", // has 4 population variants
        		"rs2691310",  // has 1 population variant 
        		"rs2949420",  // has 2 population variants
        		"rs2949421"   // has 1 population variants
        	};    	
        test(variantIDs);    	
    }    
    
    /**
     * Helper method that processes the given variants through the test pipeline.
     * 
     * @param variantIDs
     */
    private void test(String[] variantIDs) {
    	// build up the lines of non-merged variant data
    	List<String> inputLines = new ArrayList<String>();    	
    	for (String rsID: variantIDs) {
        	inputLines.addAll(mVariantData.get(rsID));    		
    	}
    	    	
		// prime pipeline
        mPipeline.setStarts(inputLines);
        
        // run pipeline, grab row by row
    	for (String rsID: variantIDs) {
            mPipeline.hasNext();
            History history = mPipeline.next();
            String json = getCompactJSON(history.get(history.size() - 1));
            
            String expectedJSON = mVariantMergedData.get(rsID); 
            
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
