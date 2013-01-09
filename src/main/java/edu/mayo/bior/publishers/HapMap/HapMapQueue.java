/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.HapMap;

import com.tinkerpop.pipes.util.Pipeline;
import java.util.Arrays;

/**
 *
 * @author m102417
 */
public class HapMapQueue {
    
    String current = "{}";
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
    
    Pipeline p = new Pipeline();
    public String constructFromOne(String jsonVariant){       
        //p.setStarts(Arrays.asList(jsonVariant));
        //String s = (String) p.next();
        //p.reset();
        return "";
    }

    private String addVariantToCurrent(String current, String jsonVariant) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
