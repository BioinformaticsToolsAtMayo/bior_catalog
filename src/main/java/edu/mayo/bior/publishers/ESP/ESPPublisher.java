package edu.mayo.bior.publishers.ESP;

import java.awt.List;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.transform.TransformFunctionPipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.bior.publishers.Cosmic.CosmicPublisher.CosmicTransformPipe;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrependStringPipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.WritePipe;
import edu.mayo.pipes.JSON.InjectIntoJsonPipe;
import edu.mayo.pipes.JSON.inject.ColumnArrayInjector;
import edu.mayo.pipes.JSON.inject.ColumnInjector;
import edu.mayo.pipes.JSON.inject.Injector;
import edu.mayo.pipes.JSON.inject.JsonType;
import edu.mayo.pipes.JSON.inject.LiteralInjector;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.bioinformatics.vocab.Type;
import edu.mayo.pipes.history.HCutPipe;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;

/**
 * 
 * @author m089716
 *
 */
public class ESPPublisher {
	
	public static void usage(){
        System.out.println("usage: ESPPublisher <rawDataFile> <catalogOutputDir>");
    }
	
	public static void main(String[] args) {	 
		ESPPublisher publisher = new ESPPublisher();      
        /*
        if(args.length >= 1){ 
            publisher.exec(args[0], args[1] + "/scratch/");
        }else{
            usage();
            System.exit(1);
        }
        */
		publisher.publish("/Users/m089716/data/esp/ESP6500SI.chr21.snps_indels.vcf", "Users/m089716/data/tmp/");
    }   
	
	public void processESPFile() {
		
	}

	/** 
	 * 
	 * @param filename - input raw data file
	 * @param outputDir - the directory where the catalog file is written
	 */
	public void publish(String filename, String outputDir) {
		final String catalogFile = "ESP6500SI_GRCh37.tsv";
    	
        System.out.println("Started loading ESP at: " + new Timestamp(new Date().getTime()));
        
        //String outfile = outputDir + "\\" + catalogFile; 
        String outfile = outputDir + catalogFile;        
        System.out.println("Outputing File to: " + outfile);
		
        Injector[] injectors = new Injector[15];
    	
    	//for(int i=0;i<headerColumns.size();i++) {    		
    	//	injectors[i] =  new ColumnInjector(i+1, headerColumns.get(i), JsonType.STRING);
    	//}
       	//Add CoreAttributes    	
        injectors[0] = new ColumnInjector(1, "CHROM", JsonType.STRING);
        injectors[1] = new ColumnInjector(2, "POS", JsonType.STRING);
        injectors[2] = new ColumnInjector(3, "ID", JsonType.STRING);
        injectors[3] = new ColumnInjector(4, "REF", JsonType.STRING);
        injectors[4] = new ColumnInjector(5, "ALT", JsonType.STRING);
        injectors[5] = new ColumnInjector(6, "QUAL", JsonType.STRING);
        injectors[6] = new ColumnInjector(7, "FILTER", JsonType.STRING);
        injectors[7] = new ColumnInjector(11, "INFO", JsonType.STRING); 
        injectors[8] = new ColumnInjector(9, CoreAttributes._type.toString(), JsonType.STRING);                
    	injectors[9] = new ColumnInjector(1, CoreAttributes._landmark.toString(), JsonType.STRING);
    	injectors[10] = new ColumnInjector(4, CoreAttributes._refAllele.toString(), JsonType.STRING);
    	injectors[11] = new ColumnArrayInjector(5, CoreAttributes._altAlleles.toString(), JsonType.STRING, ",");
    	injectors[12] = new ColumnInjector(2, CoreAttributes._minBP.toString(), JsonType.NUMBER);
    	injectors[13] = new ColumnInjector(10, CoreAttributes._maxBP.toString(), JsonType.NUMBER);
    	//injectors[31] = new ColumnInjector(32, CoreAttributes._strand.toString(), JsonType.STRING);
    	injectors[14] = new ColumnInjector(3, CoreAttributes._id.toString(),JsonType.STRING);
    	
        InjectIntoJsonPipe injectCosmicDataAsJson = new InjectIntoJsonPipe(true, injectors);
        
        // The final catalog file will have "landmark<tab>minbp<tab>maxbp<tab>JSON string"
         // Writing only columns 27: landmark/chr; 30: minbp; 31: maxbp to the final catalog file. 
        // The JSON string with all values from the raw file will be appended to the above. 
        int[] cut = new int[] {3,4,5,6,7,8,9,10,11};

        Pipe<History,History> transform = new TransformFunctionPipe<History,History>( new ESPTransformPipe() );
        
        WritePipe writePipe = new WritePipe(outfile);
		
		Pipe pipeline = new Pipeline(new CatPipe(),
                new HistoryInPipe(),
                transform,                
                injectCosmicDataAsJson,
                new HCutPipe(false, cut),
                new MergePipe("\t", true),
                new PrintPipe());
		pipeline.setStarts(Arrays.asList(filename));
		
		for(int i=0; pipeline.hasNext(); i++){
			pipeline.next();
			if (i > 3) break;				
		}
	}
	
	/**
	 * 
	 * @author m089716
	 *
	 */
	public class ESPTransformPipe implements PipeFunction<History,History> {
		
		@Override
		public History compute(History history) {
			
			//_type
            history.add(Type.VARIANT.toString());
        
            //maxBp
            history.add(history.get(1)); //TODO how to get the maxbp??
            
            //Processed INFO
            history.add(processedInfoColumn(history.get(7)));
            
			return history;
		}
		
		private String processedInfoColumn(String infoColumnData) {
			
			String processedInfo= ""; 
			
			if (infoColumnData.equals(null) || infoColumnData.equals("")) {
				processedInfo = ".";
			} else {
				String raw = infoColumnData;
				String[] rawValues = infoColumnData.split(";");
				
				Map<String, String> mapValues = new HashMap<String, String>();
				
				for(int i=0;i<rawValues.length;i++) {
					//System.out.println(rawValues[i]);
					String key = rawValues[i].substring(0,rawValues[i].indexOf("="));
					String value = rawValues[i].substring( rawValues[i].indexOf("="), rawValues[i].length());
					System.out.println(key +"::"+ value);

					mapValues.put(key, value);
					
				}
				
				//System.out.println("RAW="+Arrays.asList(rawValues));
			}
			
			//add mapValues as string to "processedInfo" and resturn it.
						
			return processedInfo;
		}
		
	}

}
