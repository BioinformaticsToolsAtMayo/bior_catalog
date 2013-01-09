/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.HapMap;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.tinkerpop.pipes.AbstractPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.bioinformatics.vocab.Type;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *
 * @author m102417
 */
public class HapMap2JSONPipe extends AbstractPipe<String,String>{
    
    String population = "UNKNOWN";
    String chrm = "UNKNOWN";
    List<String> columns = null;
    public HapMap2JSONPipe(String population, String chr, List<String> columns){
        this.population = population;
        this.columns = columns;
        this.chrm = chr;
    }
    

    @Override
    protected String processNextStart() throws NoSuchElementException {
        String s = this.starts.next();
        System.out.println(s);
        String[] split = s.split(" ");
        if(columns.size() == split.length){
            JsonObject record = new JsonObject();
            for(int i=0;i<split.length;i++){
                record.addProperty(columns.get(i), split[i]);
            }
            
//            record.addProperty(CoreAttributes._landmark.toString(), this.chrm);
//            record.addProperty("population", this.population);
//            record.addProperty(CoreAttributes._id.toString(), split[0]);//add the rs_id
//            JsonArray alts = new JsonArray();
//            String[] al = split[1].split("/");
//            record.addProperty(CoreAttributes._refAllele.toString(), al[0]);
//            for(int i=1; i<al.length;i++){//handle tri-allele
//                String allele = al[i];
//                alts.add(new JsonPrimitive(allele));
//            }
//            record.add(CoreAttributes._altAlleles.toString(), alts);
//            
//            Integer pos = new Integer(split[3]);
//            if (pos != null) {
//                //I think hapmap deals with just snps... int maxBP = new Integer(pos + split[].length() - 1);
//                record.addProperty(CoreAttributes._minBP.toString(), pos);
//                record.addProperty(CoreAttributes._maxBP.toString(), pos);
//            }
//            
//            record.addProperty(CoreAttributes._strand.toString(), split[4]);//fixme...should do some better logic to make sure strand is correct...
//            record.addProperty(CoreAttributes._type.toString(), Type.VARIANT.toString());
            
            //System.out.println(record.toString());
            return record.toString();
        }else {
            throw new NoSuchElementException();
        }
        
    }
    
}
