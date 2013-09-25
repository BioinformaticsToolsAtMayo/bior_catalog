/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.utils;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.bioinformatics.sequence.Bed2SequencePipe;
import edu.mayo.pipes.util.SystemProperties;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author m102417
 */
public class GetBasesUtil {
    
    private String GenomePath = "/data/catalogs/NCBIGenome/GRCh37.p10/hs_ref_genome.fa.tsv.bgz";
    
        /**
     * This methood is used to retrieve the REF Allele from NCBIGenome at a position.
     * In Cosmic raw data file, REF and ALT are retrieved from column:CDS Mutation
     * and this is represented using a HGVS nomenclature.. "c.123G>T"
     * but there are some cases where this data is like "c.123_124>T" where REF is missing.
     * This method below is used to retrieve REF win cases like above. 
     */
    Pipe pipeline = null;
    Bed2SequencePipe bed2sequencePipe = null;
    public String getBasePairAtPosition(String landmark, String minBP, String maxBP) {
        ArrayList<String> in = new ArrayList<String>();
        in.add(landmark);
        in.add(minBP);
        in.add(maxBP);
        String result = "";
        
        try {
	        if(bed2sequencePipe == null){
	            SystemProperties sysprop = new SystemProperties();
	            //bed2sequencePipe = new Bed2SequencePipe(sysprop.get("hs_complete_genome_catalog"));
	            bed2sequencePipe = new Bed2SequencePipe(GenomePath);
	            //bed2sequencePipe = new Bed2SequencePipe("C:\\mayo\\bior\\ncbigenome\\hs_ref_genome.fa.tsv.bgz");
	            pipeline = new Pipeline(bed2sequencePipe);
	        }
	        bed2sequencePipe.reset();
	        pipeline.reset();
	        
	        pipeline.setStarts(Arrays.asList(in));
	        
	        //if (pipeline.hasNext()) {
	        	ArrayList<String> out = (ArrayList<String>) pipeline.next();
	        	result = out.get(3); 
	        //}
        } catch(Exception e) {
        	result = "";        	
        	System.err.println(e.getMessage());
        	e.printStackTrace();
	    }
        
        return result;
    }

    
}
