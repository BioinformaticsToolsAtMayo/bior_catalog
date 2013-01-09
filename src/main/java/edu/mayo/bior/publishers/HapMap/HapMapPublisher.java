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
import edu.mayo.pipes.JSON.Delim2JSONPipe;
import edu.mayo.pipes.JSON.SimpleDrillPipe;
import edu.mayo.pipes.UNIX.*;
import edu.mayo.pipes.bioinformatics.GenbankPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.history.HistoryOutPipe;
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
        System.out.println("usage: HapMapPublisher <rawDataDir> <catalogOutputDir>");
    }
    
    public static void main(String[] args) {	 
        HapMapPublisher publisher = new HapMapPublisher();
        //publisher.publish("/data/hapmap/2010-08_phaseII+III/", "/tmp");
        System.out.println(args.length);
        if(args.length >= 1){ 
            publisher.publish(args[1], args[2] + "/scratch/");
        }else{
            usage();
            System.exit(1);
        }
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
        if(split.length > 4){
            //System.out.println(split[2]);
            return split[3];
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
        final String catalogFile = "hapmap.tsv";

        double start = System.currentTimeMillis();
    	System.out.println("Started loading HapMap at: " + new Timestamp(new Date().getTime()));
        String outfile = outputDir + "/" + catalogFile;
        System.out.println("Outputing File to: " + outfile);

        try {
            System.out.println("Parsing HapMap from: " + rawDataDir); //chrDir);            
            
            Pipeline p = new Pipeline(new LSPipe(false), new GrepPipe("^allele_freqs.*"));
            p.setStarts(Arrays.asList(new String[] {rawDataDir}));
            for(int i = 0; p.hasNext(); i++){ 
                String filename = (String)p.next();
                System.out.println("Processing File: " + filename);

                processHapMapFile(rawDataDir + "/" + filename, 
                        computeChr(filename), 
                        computePopulation(filename), 
                        computeColumns(filename, rawDataDir), 
                        new WritePipe(outfile)
                        //new PrintPipe()
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
        String[] headers = new String[18];
        headers[0] = "rsNumber";
        headers[1] = "chrom";
        headers[2] = "pos";
        headers[3] = "strand";
        headers[4] = "build"; 
        headers[5] = "center";
        headers[6] = "protLSID";
        headers[7] = "assayLSID"; 
        headers[8] = "panelLSID";
        headers[9] = "QC_code";
        headers[10] = "refallele"; 
        headers[11] = "refallele_freq"; 
        headers[12] = "refallele_count"; 
        headers[13] = "otherallele";
        headers[14] = "otherallele_freq";
        headers[15] = "otherallele_count";
        headers[16] = "totalcount";
        headers[17] = "population";//this is added via an append pipe
        Delim2JSONPipe delim2JSON = new Delim2JSONPipe(-1, false, headers, " ");
        
        String[] paths = new String[3];
        paths[0] = CoreAttributes._landmark.toString();
        paths[1] = CoreAttributes._minBP.toString();
        paths[2] = CoreAttributes._maxBP.toString();
        
        //note... need to get the population
        
        //Pipe p = new Pipeline(new CatGZPipe("gzip"), new GrepEPipe("^rs#.*"), hmj, new SimpleDrillPipe(true, paths), new MergePipe("\t", true), load);
        Pipe p = new Pipeline(new CatGZPipe("gzip"), new GrepEPipe("^rs#.*"), new AppendStringPipe(" " + population), new HistoryInPipe(), delim2JSON, new MergePipe("\t", true), new AppendStringPipe("\n"), load);
        p.setStarts(Arrays.asList(new String[] {file}));
        for(int i=0; p.hasNext(); i++){
            p.next();
            //if(i>5) break;
        }
    }
}
