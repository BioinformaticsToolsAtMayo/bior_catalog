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

/**
 *
 * @author m102417
 */
public class HapMapQueue {
    
    private String current = "{}";
    private String currentPopulation = "";
    /**
     * Takes a JSON string representing a variant, merges
     * @return 
     */
    public String mergeHapMap(String current, String jsonVariant){
        if(current.equals("{}")){
            return constructFromOne(jsonVariant);
        }else {
            return addVariantToCurrent(current, jsonVariant);
        }
    }  
    
    private JsonParser parser = new JsonParser();
    private Gson gson = new Gson();
    public String constructFromOne(String jsonVariant){           
        JsonObject dom = parser.parse(jsonVariant).getAsJsonObject();
        List<JsonObject> extractCommon = extractCommon(dom);
        JsonObject common = extractCommon.get(0);
        JsonObject specific = extractCommon.get(1);
        
        System.out.println(common);
        System.out.println(specific);
        common.add(this.currentPopulation, specific);
        System.out.println(this.currentPopulation);
        System.out.println(common);
        
        return common.toString();
    }

    public String addVariantToCurrent(String current, String jsonVariant) {
        JsonObject dom = parser.parse(current).getAsJsonObject();
        List<JsonObject> extractCommon = extractCommon(dom);
        //JsonObject common = extractCommon.get(0);
        JsonObject specific = extractCommon.get(1);
        dom.add(this.currentPopulation, specific);
        return dom.toString();
    }
    
    private static final String[] commonKeys = {"rsNumber",
                                    "chrom",
                                    "pos",
                                    "strand",
                                    "build",
                                    "refallele",
                                    "otherallele", 
                                    "_type", 
                                    "_landmark", 
                                    "_minBP", 
                                    "_maxBP", 
                                    "_strand", 
                                    "_refAllele", 
                                    "_altAlleles", 
                                    "_id"};
    private static final ArrayList<String> cKeys = new ArrayList(Arrays.asList(commonKeys));
    private HashMap knownPopulations = new HashMap();
    public List<JsonObject> extractCommon(JsonObject dom){
        //System.out.println("extractCommon");
        JsonObject common = new JsonObject();
        JsonObject specific = new JsonObject();
        //common = new HashMap();
        //specific = new HashMap();
        Iterator<Entry<String,JsonElement>> it = dom.entrySet().iterator();
        for(int i=0; it.hasNext(); i++){
             Entry<String,JsonElement> entry = it.next();
             String key = entry.getKey();
             JsonElement value = entry.getValue();
             //System.out.println(key);
             //System.out.println(value.toString());
            if(cKeys.contains(key)){
                addElegant(common, key, value);
            }else if(key.equals("population")){
                this.currentPopulation = (String) dom.get("population").getAsString();
                knownPopulations.put(key, true);
            }else if(knownPopulations.containsKey(key)){ //if this key is a population we have seen so far, ignore it...
                ;
            }else {
            	System.out.println("  key: " + key + ", " + value);
                addElegant(specific, key, value);
                System.out.println("    done.");
                //specific.put(key, hm.get(key));
            }
        }
        return Arrays.asList(common, specific);
    }
    
    public JsonObject addElegant(JsonObject jo, String key, JsonElement value){
    	String valStr = value.toString();
    	//System.out.println(  "jsonValue.toString(): " + value.toString());
    	//System.out.println(  "jsonValue.getAsString(): " + value.getAsString());
    	if(JSONUtil.isInt(valStr)&& !key.equals(CoreAttributes._landmark.toString())){ 
            jo.addProperty(key, value.getAsInt());
        }else if(JSONUtil.isDouble(valStr)&& !key.equals(CoreAttributes._landmark.toString())){ 
            jo.addProperty(key, value.getAsDouble());
        }else {
            jo.addProperty(key, valStr);
        }
        return jo;
    }
}
