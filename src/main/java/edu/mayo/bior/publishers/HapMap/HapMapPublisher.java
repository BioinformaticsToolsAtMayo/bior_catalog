/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.HapMap;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.bior.publishers.NCBIGene.NCBIGenePublisher;
import edu.mayo.pipes.*;
import edu.mayo.pipes.JSON.BioJavaRichSequence2JSON;
import edu.mayo.pipes.JSON.SimpleDrillPipe;
import edu.mayo.pipes.UNIX.*;
import edu.mayo.pipes.bioinformatics.GenbankPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.util.GenomicObjectUtils;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author m102417
 */
public class HapMapPublisher {
        public static void usage(){
        System.out.println("usage: HapMap Publisher <rawDataDir> <catalogOutputDir>");
    }
    
    public static void main(String[] args) {	 
        HapMapPublisher publisher = new HapMapPublisher();
        publisher.publish("/Volumes/data4/bsi/refdata-new/variation/hapmap/human/ncbi36/downloaded/genotypes/2010-08_phaseII+III/forward/2012_09_24", "/tmp");
//        System.out.println(args.length);
//        if(args.length >= 1){
//            publisher.publish(args[0], args[1]);
//        }else{
//            usage();
//            System.exit(1);
//        }
    } 
    
    public List<String> computeColumns(String filename, String directory) throws Exception{
        String fullFile = directory + "/" + filename;
        Pipeline p = new Pipeline(new CatGZPipe("gzip"), new GrepPipe("^rs#.*"), new SplitPipe(" ") );//new PrintPipe()
        p.setStarts(Arrays.asList(fullFile));
        for(int i=0;p.hasNext();i++){
            return (List<String>)p.next();
            //if(i>2) break;
        }
        throw new Exception("Can't calculate header in file: " + fullFile);
    }
    
    /* files are named something like: genotypes_chr10_ASW_r28_nr.b36_fwd.txt.gz */
    public String computePopulation(String filename) throws Exception{
        String[] split = filename.split("_");
        if(split.length > 3){
            //System.out.println(split[2]);
            return split[2];
        }
        throw new Exception("filename did not contain a population");
    }
    
    /* files are named something like: genotypes_chr10_ASW_r28_nr.b36_fwd.txt.gz */
    public String computeChr(String filename) throws Exception{
        String[] split = filename.split("_");
        if(split.length > 2){
            String landmark = split[1].replaceFirst("chr", "");
            //System.out.println(landmark);
            return landmark;
        }
        throw new Exception("filename did not contain a chromosome");
    }

    public void publish(String rawDataDir, String outputDir) {
        final String catalogFile = "variants.tsv";

        double start = System.currentTimeMillis();
    	System.out.println("Started loading HapMap at: " + new Timestamp(new Date().getTime()));
        String outfile = outputDir + "/" + catalogFile;
        System.out.println("Outputing File to: " + outfile);

        try {
            System.out.println("Parsing HapMap from: " + rawDataDir); //chrDir);            
            
            Pipeline p = new Pipeline(new Pipe[] {new LSPipe(false)});
            p.setStarts(Arrays.asList(new String[] {rawDataDir}));
            for(int i = 0; p.hasNext(); i++){ 
                String filename = (String)p.next();
                System.out.println("Processing File: " + filename);

                processHapMapFile(rawDataDir + "/" + filename, 
                        computeChr(filename), 
                        computePopulation(filename), 
                        computeColumns(filename, rawDataDir), 
                        //new WritePipe(outfile)
                        new PrintPipe()
                        );
                //break;
            }
        } catch (Exception ex) {
            Logger.getLogger(HapMapPublisher.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        }        
        System.out.println("Completed loading HapMap at: " + new Timestamp(new Date().getTime()));
        double end = System.currentTimeMillis();
        System.out.println("Runtime: " + (end-start)/1000.0);
    }
    
    private void processHapMapFile(String file, String chr, String population, List<String> header,  Pipe load) {      
        HapMap2JSONPipe hmj = new HapMap2JSONPipe(population, chr, header); 
        String[] paths = new String[3];
        paths[0] = CoreAttributes._landmark.toString();
        paths[1] = CoreAttributes._minBP.toString();
        paths[2] = CoreAttributes._maxBP.toString();
        
        Pipe p = new Pipeline(new CatGZPipe("gzip"), new GrepEPipe("^rs#.*"), hmj, new SimpleDrillPipe(true, paths), new MergePipe("\t", true), load);
        p.setStarts(Arrays.asList(new String[] {file}));
        for(int i=0; p.hasNext(); i++){
            p.next();
            break;
        }
    }
}
