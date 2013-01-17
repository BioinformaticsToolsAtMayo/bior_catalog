/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.HapMap;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.util.JSONUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * @author m102417
 */
public class HapMapQueue {
    
    private String current = "{}";
    private String currentPopulation = "";

    private static final String[] commonKeys = {
		"rsNumber",
		"chrom",
		"pos",
		"strand",
		"build",
		"refallele",
		"otherallele", 
		CoreAttributes._type.toString(), 
		CoreAttributes._landmark.toString(), 
		CoreAttributes._minBP.toString(), 
		CoreAttributes._maxBP.toString(), 
		CoreAttributes._strand.toString(), 
		CoreAttributes._refAllele.toString(), 
		CoreAttributes._altAlleles.toString(),
		CoreAttributes._id.toString()
		};
    private static final ArrayList<String> cKeys = new ArrayList(Arrays.asList(commonKeys));
    private HashMap knownPopulations = new HashMap();
    private JsonParser parser = new JsonParser();
    private Gson gson = new Gson();

    
    public String constructFromOne(String jsonVariant){           
        JsonObject dom = parser.parse(jsonVariant).getAsJsonObject();
        List<JsonObject> extractCommon = extractCommon(dom);
        JsonObject common = extractCommon.get(0);
        JsonObject population = extractCommon.get(1);
        
        common.add(this.currentPopulation, population);
        
        return common.toString();
    }

    /**
     * Takes a JSON string representing a variant, merges with another
     * <b>Both must already have their populations collapsed (by calling constructFromOne()</b> 
     * @return 
     */
    public String mergeHapMap(String variantJson1, String variantJson2){
    	String mergedVariant = "";
    	if(variantJson1.equals("{}")){
            mergedVariant = variantJson2;
        }else {
        	// Pull out the population from the second variant and add to first variant's DOM
            JsonObject dom1 = parser.parse(variantJson1).getAsJsonObject();

            JsonObject dom2 = parser.parse(variantJson2).getAsJsonObject();
            List<JsonObject> domList2 = extractCommon(dom2);
            if( domList2.size() <= 1 || ! domList2.iterator().hasNext())
            	return dom1.toString();

            // Add population from 2nd json object to 1st
            JsonObject population2 = domList2.get(1);
            Set<Entry<String,JsonElement>> populationSet =  population2.entrySet();
            Entry<String,JsonElement> entry = populationSet.iterator().next();
            addElegant(dom1, entry.getKey(), entry.getValue());
            mergedVariant = dom1.toString();
        }
    	return mergedVariant;
    }

    public List<JsonObject> extractCommon(JsonObject dom){
        JsonObject common = new JsonObject();
        JsonObject specific = new JsonObject();
        Iterator<Entry<String,JsonElement>> it = dom.entrySet().iterator();
        for(int i=0; it.hasNext(); i++){
             Entry<String,JsonElement> entry = it.next();
             String key = entry.getKey();
             JsonElement value = entry.getValue();
            if(cKeys.contains(key)){
                addElegant(common, key, value);
            }else if(key.equals("population")){
                this.currentPopulation = (String) dom.get("population").getAsString();
                knownPopulations.put(key, true);
            }else if(knownPopulations.containsKey(key)){ //if this key is a population we have seen so far, ignore it...
                ;
            }else {
                addElegant(specific, key, value);
            }
        }
        return Arrays.asList(common, specific);
    }
    
    public JsonObject addElegant(JsonObject jo, String key, JsonElement value){
    	String valStr = getJsonElementValAsString(value);
    	
    	// If _landmark core attribute, then treat as a string rather than integer
    	// since we will still have "X" and "Y"
    	boolean isLandmark = key.equals(CoreAttributes._landmark.toString());
    	if(JSONUtil.isInt(valStr) && ! isLandmark){ 
            jo.addProperty(key, value.getAsInt());
        }else if(JSONUtil.isDouble(valStr) && ! isLandmark){ 
            jo.addProperty(key, value.getAsDouble());
        }else if(valStr.startsWith("{") && valStr.endsWith("}")) {
        	jo.add(key, value);
        }else {
        	jo.addProperty(key, valStr);
        }
        return jo;
    }
    
    private String getJsonElementValAsString(JsonElement value) {
    	// Try using the getAsString() method - this sometimes errors out as in the case of nested JSON
    	String valStr = value.toString();
    	try {
    		valStr = value.getAsString();
    	} catch(UnsupportedOperationException e) {
    		// If we get this exception, leave valStr as value.toString()
    		// This will happen in the case of a nested value such as A:{B:1}
    	}
    	return valStr;
    }
}
