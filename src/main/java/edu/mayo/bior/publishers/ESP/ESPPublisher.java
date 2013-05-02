package edu.mayo.bior.publishers.ESP;

import java.io.File;
import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.transform.TransformFunctionPipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.WritePipe;
import edu.mayo.pipes.JSON.DrillPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.UNIX.GrepPipe;
import edu.mayo.pipes.UNIX.LSPipe;
import edu.mayo.pipes.bioinformatics.VCF2VariantPipe;
import edu.mayo.pipes.history.HCutPipe;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;

/**
 * 
 * @author m089716 Surendra Konathala
 *
 */
public class ESPPublisher {
	private static Logger sLogger = Logger.getLogger(ESPPublisher.class);
	
	public static void usage(){
        System.out.println("usage: ESPPublisher <rawDataDir> <ESPTsvOutfile>");
    }
	
	public static void main(String[] args) {	 
		try {
			ESPPublisher publisher = new ESPPublisher();      
	        
	        if(args.length >= 1){ 
	            publisher.publish(args[0], args[1]);
	        }else{
	            usage();
	            System.exit(1);
	        }
			
			//publisher.publish("/Users/m089716/data/esp", "/Users/m089716/data/tmp/ESP6500SI_GRCh37.tsv");			
		}catch(Exception e) {
    		e.printStackTrace();
    	}        
    }   
	
	
	/** 
	 * 
	 * @param filename - input raw data file
	 * @param outputDir - the directory where the catalog file is written
	 */
	public void publish(String rawDataDir, String outfile) throws FileNotFoundException {
		
		verifyInputDirAndOutputFile(rawDataDir, outfile);
    	
    	double start = System.currentTimeMillis();
    	System.out.println("Started loading ESP at: " + new Timestamp(new Date().getTime()));
        System.out.println("Outputing File to: " + outfile);

        try {
            System.out.println("Parsing ESP from: " + rawDataDir); //chrDir);            
            
            Pipeline p = new Pipeline(new LSPipe(false), new GrepPipe("ESP6500SI.chr*.*"));
            p.setStarts(Arrays.asList(new String[] {rawDataDir}));
            for(int i = 0; p.hasNext(); i++){ 
                String filename = (String)p.next();
                System.out.println("Processing File: " + filename);

                processESPFile(rawDataDir + "/" + filename,                        
                        new WritePipe(outfile)
                        //new PrintPipe()
                        );
            }
        } catch (Exception ex) {
            sLogger.error("Failed to publish ESP catalog!", ex);
            ex.printStackTrace();
        }        
        System.out.println("Completed loading ESP at: " + new Timestamp(new Date().getTime()));
        double end = System.currentTimeMillis();
        System.out.println("Runtime: " + (end-start)/1000.0);
	}
	
	/** */
	public void processESPFile(String filename, Pipe load) {
		
        int[] cut = new int[] {3,4,5,6,7,8,10,11};
        
        String[] paths = {"_maxBP", "INFO.MAF"};             
        
        Pipe<History,History> transform = new TransformFunctionPipe<History,History>( new ESPTransformPipe() );
        
		Pipe pipeline = new Pipeline(new CatPipe(),
				new HistoryInPipe(),
				new VCF2VariantPipe(),				
				new DrillPipe(true, paths),				
				transform,
				new HCutPipe(false, cut),
				new MergePipe("\t", true), 
				load
				);
		pipeline.setStarts(Arrays.asList(filename));
		
		for(int i=0; pipeline.hasNext(); i++){
			pipeline.next();
			//if (i > 1) break;				
		}
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
	
	/**	Transforms/splits the RAW MAF data in INFO column from ["0.02","0.213","0.003"] 
	 * to "EA":{"_maf":[0.0026]},"AA":{"_maf":[0.0094]},"ALL":{"_maf":[0.002]}} 
	*/
	public class ESPTransformPipe implements PipeFunction<History,History> {
		public String MAF_EA = "";
		public String MAF_AA = "";
		public String MAF_ALL = "";
		
		final String[] MAF_LITERALS = { "EA", "AA", "ALL"};	
		
		@Override
		public History compute(History history) {			
			//Processed INFO
			history.add(processInfo(history.get(9), history.get(10)));
						
			return history;
		}		

		/**
		 * 
		 * @param rawMAFLine - ["0.02","0.213","0.003"]
		 * @param infoLine - {"CHROM":"12",.....}
		 * @return - infoLine with added MAF values as {"CHROM":"12"....,"EA":{"_maf":[0.0026]},"AA":{"_maf":[0.0094]},"ALL":{"_maf":[0.002]}}
		 */
		private String processInfo(String rawMAFLine, String infoLine) {
			
		    JsonParser jparser = new JsonParser();
		    JsonObject root = jparser.parse(infoLine).getAsJsonObject();
		    	
		    JsonObject proot = this.processMAF(rawMAFLine, root);
		
		    return proot.toString();
		}

		/** */
		private JsonObject processMAF(String rawMAFLine, JsonObject proot) {			
			if (rawMAFLine.equals(null) || rawMAFLine.equals("")) {
				//processedInfo = ".";
			} else {
				String trimRawMAFLine = rawMAFLine.replaceAll("\\[|\\]", ""); //remove [ ]
				trimRawMAFLine = trimRawMAFLine.replaceAll("\"", ""); // remove "
				
				String[] rawValues = trimRawMAFLine.split(",");				
				//System.out.println("RV="+Arrays.asList(rawValues));
				
				JsonArray arr;
				JsonObject record;
				int i=0;
				for(String val : rawValues) {
					arr = new JsonArray();
					try{
						Double score = new Double(val);
						//String tvalue = BigDecimal.valueOf(score.doubleValue() / 100).toPlainString();
						BigDecimal bd = new BigDecimal(score);
						BigDecimal newVal = bd.divide(new BigDecimal(100), 6, RoundingMode.HALF_EVEN);
						//arr.add(new JsonPrimitive(Double.parseDouble(tvalue)));
						arr.add(new JsonPrimitive(newVal));
						
					} catch(NumberFormatException nfe) {
						arr.add(new JsonPrimitive(val));
					}			
					
					record = new JsonObject();					
					record.add("_maf", arr);
					
					proot.add(MAF_LITERALS[i], record);
					i++;					
				}				
			}
			
			return proot;
		}
	}			
}
