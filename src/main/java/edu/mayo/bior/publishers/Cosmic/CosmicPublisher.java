/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.Cosmic;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.bior.publishers.HapMap.HapMap2JSONPipe;
import edu.mayo.bior.publishers.HapMap.HapMapPublisher;
import edu.mayo.pipes.AppendStringPipe;
import edu.mayo.pipes.HeaderPipe;
import edu.mayo.pipes.JSON.Delim2JSONPipe;
import edu.mayo.pipes.JSON.InjectIntoJsonPipe;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrependStringPipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.UNIX.CatGZPipe;
import edu.mayo.pipes.UNIX.GrepEPipe;
import edu.mayo.pipes.UNIX.GrepPipe;
import edu.mayo.pipes.UNIX.LSPipe;
import edu.mayo.pipes.WritePipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.bioinformatics.vocab.Type;
import edu.mayo.pipes.history.HistoryInPipe;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author m102417
 */
public class CosmicPublisher {
        public static void usage(){
        System.out.println("usage: CosmicPublisher <rawDataFile> <catalogOutputDir>");
    }
    
    public static void main(String[] args) {	 
        CosmicPublisher publisher = new CosmicPublisher();
        publisher.publish("/data/cosmic/v62/", "/tmp");
//        System.out.println(args.length);
//        if(args.length >= 1){ 
//            publisher.publish(args[1], args[2] + "/scratch/");
//        }else{
//            usage();
//            System.exit(1);
//        }
    } 
    
    public void getHeader(String rawDataDir){
        Pipe p = new Pipeline(new LSPipe(false), 
                             new PrependStringPipe(rawDataDir),
                             new CatGZPipe("gzip"),
                             new HeaderPipe(1),
                             new HistoryInPipe(),
                             new PrintPipe());
        p.setStarts(Arrays.asList(rawDataDir));
        for(int i=0;p.hasNext();i++){
            p.next();
            if(i>100) break;
        }
    }
    
    public void publish(String rawDataDir, String outputDir) {
        final String catalogFile = "cosmic.tsv";
        getHeader(rawDataDir);
        InjectIntoJsonPipe inject = new InjectIntoJsonPipe(8, new AbstractMap.SimpleEntry(CoreAttributes._type.toString(), Type.VARIANT.toString()), 
                                                      new AbstractMap.SimpleEntry("1",CoreAttributes._landmark.toString()), 
                                                      new AbstractMap.SimpleEntry("2", CoreAttributes._minBP.toString()),
                                                      new AbstractMap.SimpleEntry("3", CoreAttributes._maxBP.toString()),
                                                      new AbstractMap.SimpleEntry("4", CoreAttributes._strand.toString()),
                                                      new AbstractMap.SimpleEntry("5", CoreAttributes._refAllele.toString()),
                                                      new AbstractMap.SimpleEntry("6", CoreAttributes._altAlleles.toString()),
                                                      new AbstractMap.SimpleEntry("7", CoreAttributes._id.toString())
            );
        double start = System.currentTimeMillis();
        System.out.println("Started loading HapMap at: " + new Timestamp(new Date().getTime()));
        String outfile = outputDir + "/" + catalogFile;
        System.out.println("Outputing File to: " + outfile);
        Pipe p = new Pipeline(new LSPipe(false), 
                             new PrependStringPipe(rawDataDir),
                             new CatGZPipe("gzip"),
                             new HeaderPipe(1),
                             new HistoryInPipe(),
                             inject,
                             new PrintPipe());
        p.setStarts(Arrays.asList(rawDataDir));
        for(int i=0;p.hasNext();i++){
            p.next();
            if(i>100) break;
        }
    }
    

//    public void publish(String rawDataDir, String outputDir) {
//        final String catalogFile = "hapmap.tsv";
//
//        double start = System.currentTimeMillis();
//    	System.out.println("Started loading HapMap at: " + new Timestamp(new Date().getTime()));
//        String outfile = outputDir + "/" + catalogFile;
//        System.out.println("Outputing File to: " + outfile);
//
//        try {
//            System.out.println("Parsing HapMap from: " + rawDataDir); //chrDir);            
//            
//            Pipeline p = new Pipeline(new LSPipe(false), new GrepPipe("^allele_freqs.*"));
//            p.setStarts(Arrays.asList(new String[] {rawDataDir}));
//            for(int i = 0; p.hasNext(); i++){ 
//                String filename = (String)p.next();
//                System.out.println("Processing File: " + filename);
//
//                processHapMapFile(rawDataDir + "/" + filename, 
//                        computeChr(filename), 
//                        computePopulation(filename), 
//                        computeColumns(filename, rawDataDir), 
//                        new WritePipe(outfile)
//                        //new PrintPipe()
//                        );
//                //break;
//            }
//        } catch (Exception ex) {
//            Logger.getLogger(HapMapPublisher.class.getName()).log(Level.SEVERE, null, ex);
//            ex.printStackTrace();
//        }        
//        System.out.println("Completed loading HapMap at: " + new Timestamp(new Date().getTime()));
//        double end = System.currentTimeMillis();
//        System.out.println("Runtime: " + (end-start)/1000.0);
//    }
//    
//    private void processHapMapFile(String file, String chr, String population, List<String> header,  Pipe load) { 
//        HapMap2JSONPipe hmj = new HapMap2JSONPipe(population, chr, header); 
//        String[] headers = new String[18];
//        headers[0] = "rsNumber";
//        headers[1] = "chrom";
//        headers[2] = "pos";
//        headers[3] = "strand";
//        headers[4] = "build"; 
//        headers[5] = "center";
//        headers[6] = "protLSID";
//        headers[7] = "assayLSID"; 
//        headers[8] = "panelLSID";
//        headers[9] = "QC_code";
//        headers[10] = "refallele"; 
//        headers[11] = "refallele_freq"; 
//        headers[12] = "refallele_count"; 
//        headers[13] = "otherallele";
//        headers[14] = "otherallele_freq";
//        headers[15] = "otherallele_count";
//        headers[16] = "totalcount";
//        headers[17] = "population";//this is added via an append pipe
//        Delim2JSONPipe delim2JSON = new Delim2JSONPipe(-1, false, headers, " ");
//        
//        String[] paths = new String[3];
//        paths[0] = CoreAttributes._landmark.toString();
//        paths[1] = CoreAttributes._minBP.toString();
//        paths[2] = CoreAttributes._maxBP.toString();
//        
//        //note... need to get the population
//        
//        //Pipe p = new Pipeline(new CatGZPipe("gzip"), new GrepEPipe("^rs#.*"), hmj, new SimpleDrillPipe(true, paths), new MergePipe("\t", true), load);
//        Pipe p = new Pipeline(new CatGZPipe("gzip"), new GrepEPipe("^rs#.*"), new AppendStringPipe(" " + population), new HistoryInPipe(), delim2JSON, new MergePipe("\t", true), new AppendStringPipe("\n"), load);
//        p.setStarts(Arrays.asList(new String[] {file}));
//        for(int i=0; p.hasNext(); i++){
//            p.next();
//            //if(i>5) break;
//        }
//    }
}
