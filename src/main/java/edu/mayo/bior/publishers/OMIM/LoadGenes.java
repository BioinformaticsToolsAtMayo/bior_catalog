package edu.mayo.bior.publishers.OMIM;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.transform.TransformFunctionPipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.JSON.Delim2JSONPipe;

import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.util.SystemProperties;
import java.util.ArrayList;
import java.util.List;

public class LoadGenes {
	
	private static String version = "unkown";		
	public static void main(String[] args) throws IOException{

        //Used only for version
        String mim2gene = "/data/OMIM/genemap";
        System.out.println("Publishing: " + mim2gene);
        LoadGenes publisher = new LoadGenes();
        //TODO: switch out for the actual insert pipe
        publisher.exec(mim2gene, new PrintPipe());       
    }
    
        
      /**
        1  - Numbering system, in the format  Chromosome.Map_Entry_Number
        2  - Month entered
        3  - Day     "
        4  - Year    "
        5  - Cytogenetic location
        6  - Gene Symbol(s)
        7  - Gene Status (see below for codes)
        8  - Title
        9  - Title, cont.
        10 - MIM Number
        11 - Method (see below for codes)
        12 - Comments
        13 - Comments, cont.
        14 - Disorders (each disorder is followed by its MIM number, if
                different from that of the locus, and phenotype mapping method (see
                below).  Allelic disorders are separated by a semi-colon.
        15 - Disorders, cont.
        16 - Disorders, cont.
        17 - Mouse correlate
        18 - Reference
    */    
    public void exec(String filename, Pipe insert){
        //Pipe<String[],List<Gene>> transform = new TransformFunctionPipe<String[],List<Gene>>(new LoadGenes.OMIM2Genes());
        String[] headers = {
            "Chromosome.Map_Entry_Number",
            "MonthEntered",
            "Day",
            "Year",
            "Cytogenetic_location",
            "GeneSymbols",
            "Gene_Status",
            "Title",
            "Title_cont",
            "MIM_Number",
            "Method",
            "Comments",
            "Comments",
            "Disorders",
            "Disorders_cont",
            "Disorders_cont",
            "Mouse_correlate",
            "Reference"
        };
        //TODO: Gene Symbols need to be an array...
        Delim2JSONPipe pipes2json = new Delim2JSONPipe(-1, false,  headers, "|");
    	Pipe pipeline = new Pipeline(new CatPipe(),
                                    new HistoryInPipe(),
                                    pipes2json,
                                    insert);
    	pipeline.setStarts(Arrays.asList(filename));
    	while(pipeline.hasNext()){
    		pipeline.next();
    	}
    }
    


}
