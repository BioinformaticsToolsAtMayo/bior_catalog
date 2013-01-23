/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.HapMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.AppendStringPipe;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.WritePipe;
import edu.mayo.pipes.JSON.Delim2JSONPipe;
import edu.mayo.pipes.UNIX.CatGZPipe;
import edu.mayo.pipes.UNIX.GrepEPipe;
import edu.mayo.pipes.UNIX.GrepPipe;
import edu.mayo.pipes.UNIX.LSPipe;
import edu.mayo.pipes.history.HistoryInPipe;

/** @author m102417 Daniel Quest,  Michael Meiners */
public class HapMapPublisher {
    public static void usage(){
        System.out.println("usage: HapMapPublisher <rawDataDir> <hapmapTsvOutfile>");
    }
    
    public static void main(String[] args) {
    	try {
	        HapMapPublisher publisher = new HapMapPublisher();
	        if(args.length >= 1){ 
	            publisher.publish(args[0], args[1]);
	        }else{
	            usage();
	            System.exit(1);
	        }
    	}catch(Exception e) {
    		e.printStackTrace();
    	}
    } 
    

    public void publish(String rawDataDir, String outfile) throws FileNotFoundException {
    	verifyInputDirAndOutputFile(rawDataDir, outfile);
    	
    	double start = System.currentTimeMillis();
    	System.out.println("Started loading HapMap at: " + new Timestamp(new Date().getTime()));
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
            }
        } catch (Exception ex) {
            Logger.getLogger(HapMapPublisher.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        }        
        System.out.println("Completed loading HapMap at: " + new Timestamp(new Date().getTime()));
        double end = System.currentTimeMillis();
        System.out.println("Runtime: " + (end-start)/1000.0);
    }
    
	/** Verify that the rawDataDir exists and is a directory */
	private void verifyInputDirAndOutputFile(String rawDataDir, String outfile) throws FileNotFoundException {
    	if( ! new File(rawDataDir).exists() || ! new File(rawDataDir).isDirectory() ) {
    		String msg = "Input directory does not exist or is not a directory: " + rawDataDir;
    		System.err.println(msg);
    		throw new FileNotFoundException(msg);
    	}
    	
    	// outfile should NOT be a directory
    	if(new File(outfile).exists() && new File(outfile).isDirectory())
    		throw new FileNotFoundException("Output file must be a file, not a directory: " + outfile);
    	
    	// outfile's parent directory must exist
    	File outputDir = new File(outfile).getParentFile();
    	if(! outputDir.exists())
    		throw new FileNotFoundException("The parent directory for the output file does not exist: " + outputDir);
    	
        // Delete the file if it already exists so we start with an empty file
    	if(new File(outfile).exists())
    		new File(outfile).delete();
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

    
    private void processHapMapFile(String file, String chr, String population, List<String> header,  Pipe load) { 
        //HapMap2JSONPipe hmj = new HapMap2JSONPipe(population, chr, header); 
        String[] headers = new String[] {
        		"rsNumber",
		        "chrom",
		        "pos",
		        "strand",
		        "build", 
		        "center",
		        "protLSID",
		        "assayLSID", 
		        "panelLSID",
		        "QC_code",
		        "refallele", 
		        "refallele_freq", 
		        "refallele_count", 
		        "otherallele",
		        "otherallele_freq",
		        "otherallele_count",
		        "totalcount",
		        "population" //this is added via an append pipe
        };
        Delim2JSONPipe delim2JSON = new Delim2JSONPipe(-1, false, headers, " ");
        
        Pipe p = new Pipeline(new CatGZPipe("gzip"), 
        					  new GrepEPipe("^rs#.*"), 
        					  new AppendStringPipe(" " + population), 
        					  new HistoryInPipe(), 
        					  delim2JSON, 
        					  new MergePipe("\t", true), 
        					  load);
        p.setStarts(Arrays.asList(new String[] {file}));
        for(int i=0; p.hasNext(); i++){
            p.next();
        }
    }
}
