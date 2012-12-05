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
import edu.mayo.pipes.JSON.SimpleDrillPipe;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.UNIX.GrepPipe;
import edu.mayo.pipes.UNIX.LSPipe;
import edu.mayo.pipes.WritePipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
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
    
    public static void usage(){
        System.out.println("usage: NCBIGenePublisher <rawDataDir> <catalogOutputDir>");
    }
    
    public static void main(String[] args) {	 
        NCBIGenePublisher publisher = new NCBIGenePublisher();
        System.out.println(args.length);
        //publisher.exec("/tmp/"); //
        if(args.length >= 1){
            publisher.publish(args[0], args[1]);
        }else{
            usage();
        }
    } 

    public void publish(String rawDataDir, String outputDir) {
        final String geneCatalogFile = "genes.tsv";

    	System.out.println("Started loading NCBIGenes.. at:" + new Timestamp(new Date().getTime()));
        String outfile = outputDir + "/" + geneCatalogFile;
        System.out.println("Outputing File to: " + outfile);

		//SystemProperties sysprop;
        try {
//            sysprop = new SystemProperties();
//            //note, the chrDir is a directory that is constructed using the bin/uncompressGenes.bash (called after the download)
//            String chrDir = sysprop.get("bior.catalog.ncbigene.chrdir");
            System.out.println("Parsing Genes from: " + rawDataDir); //chrDir);            
            
            //Pipeline p = new Pipeline(new LSPipe(false), new GrepPipe(".*gbs.txt"));
            Pipeline p = new Pipeline(new Pipe[] {new LSPipe(false), new GrepPipe(".*gbs.txt")});
            p.setStarts(Arrays.asList(new String[] {rawDataDir}));
            for(int i = 0; p.hasNext(); i++){ 
                String filename = (String)p.next();
                System.out.println("Processing File: " + filename);
                String chrstr = filename.replaceAll(".gbs.txt", "");
                String c = GenomicObjectUtils.computechr(chrstr); 
                //System.out.println(c);
                //processGenes(chrDir + filename, c, new PrintPipe());
                processGenes(rawDataDir + "/" + filename, c, new WritePipe(outfile));
                
                
            }
        } catch (Exception ex) {
            Logger.getLogger(NCBIGenePublisher.class.getName()).log(Level.SEVERE, null, ex);
            //System.out.println("Error processing NCBIGenes: " + ex);
            ex.printStackTrace();
            //throw new Exception(ex);
        }        
        System.out.println("Completed loading NCBIGenes.. at:" + new Timestamp(new Date().getTime()));
	}
    
    private void processGenes(String chrFile, String chr, Pipe load) {
        String[] featureTypes = new String[1];
        featureTypes[0] = "gene"; //CDS, mRNA, exon, ...
        BioJavaRichSequence2JSON bj = new BioJavaRichSequence2JSON(chr, featureTypes); //just a placeholder...
        String[] paths = new String[3];
        paths[0] = CoreAttributes._landmark.toString();
        paths[1] = CoreAttributes._minBP.toString();
        paths[2] = CoreAttributes._maxBP.toString();
        
        Pipe p = new Pipeline(new GenbankPipe(), bj, new DrainPipe(), new SimpleDrillPipe(true, paths), new MergePipe("\t", true), load);
        p.setStarts(Arrays.asList(new String[] {chrFile}));
        for(int i=0; p.hasNext(); i++){
            p.next();
        }
    }
}
