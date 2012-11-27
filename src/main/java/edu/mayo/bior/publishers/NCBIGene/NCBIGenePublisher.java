/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.NCBIGene;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.transform.IdentityPipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.DrainPipe;
import edu.mayo.pipes.JSON.BioJavaRichSequence2JSON;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.UNIX.GrepPipe;
import edu.mayo.pipes.UNIX.LSPipe;
import edu.mayo.pipes.bioinformatics.GenbankPipe;
import edu.mayo.pipes.util.GenomicObjectUtils;
import edu.mayo.pipes.util.SystemProperties;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  This class creates all the catalogs needed for NCBIGene.
 *  It assumes you have already called:
 *      bior_catalog/downloaders/downloadGenes.bash
 *      bior_catalog/downloaders/uncompressGenes.bash
 * 
 * those scripts will download the raw information, and uncompress/grep it into something
 * more usable.  Assume that this usable stuff is in:
 * 
 * $BIOR_CATALOG_HOME/NCBIGene/version/scratch
 * 
 * @author m102417
 */
public class NCBIGenePublisher {
        public static void main(String[] args) {	
        NCBIGenePublisher publisher = new NCBIGenePublisher();
        publisher.exec();
    } 

    public void exec() {
    	//System.out.println("Started loading NCBIGenes.. at:" + new Timestamp(new Date().getTime()));

		SystemProperties sysprop;
        try {
            sysprop = new SystemProperties();
            //note, the chrDir is a directory that is constructed using the bin/uncompressGenes.bash (called after the download)
            String chrDir = sysprop.get("bior.catalog.ncbigene.chrdir");
            System.out.println("Parsing Genes from: " + chrDir);            
            
            //Pipeline p = new Pipeline(new LSPipe(false), new GrepPipe(".*gbs.txt"));
            Pipeline p = new Pipeline(new LSPipe(false), new GrepPipe(".*gbs.txt"));
            p.setStarts(Arrays.asList(chrDir));
            for(int i = 0; p.hasNext(); i++){ 
                String filename = (String)p.next();
                System.out.println("Processing File: " + filename);
                String chrstr = filename.replaceAll(".gbs.txt", "");
                String c = GenomicObjectUtils.computechr(chrstr); 
                //System.out.println(c);
                process(chrDir + filename, c, new PrintPipe());
            }
        } catch (Exception ex) {
            Logger.getLogger(NCBIGenePublisher.class.getName()).log(Level.SEVERE, null, ex);
            //System.out.println("Error processing NCBIGenes: " + ex);
            ex.printStackTrace();
            //throw new Exception(ex);
        }        
        System.out.println("Completed loading NCBIGenes.. at:" + new Timestamp(new Date().getTime()));
	}
    
    private void process(String chrFile, String chr, Pipe load) {
        String[] featureTypes = new String[1];
        featureTypes[0] = "gene"; //CDS, mRNA, exon, ...
        BioJavaRichSequence2JSON bj = new BioJavaRichSequence2JSON(chr, featureTypes); //just a placeholder...
        
        Pipe p = new Pipeline(new GenbankPipe(), bj, new DrainPipe(), load);
        p.setStarts(Arrays.asList(chrFile));
        for(int i=0; p.hasNext(); i++){
            p.next();
        }
    }
}
