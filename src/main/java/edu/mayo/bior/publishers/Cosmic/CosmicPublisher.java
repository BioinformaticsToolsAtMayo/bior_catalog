/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.Cosmic;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.transform.TransformFunctionPipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.HeaderPipe;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.WritePipe;
import edu.mayo.pipes.JSON.InjectIntoJsonPipe;
import edu.mayo.pipes.JSON.inject.ColumnArrayInjector;
import edu.mayo.pipes.JSON.inject.ColumnInjector;
import edu.mayo.pipes.JSON.inject.Injector;
import edu.mayo.pipes.JSON.inject.JsonType;
import edu.mayo.pipes.JSON.inject.LiteralInjector;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.UNIX.CatGZPipe;
import edu.mayo.pipes.UNIX.HeadPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.bioinformatics.vocab.Type;
import edu.mayo.pipes.bioinformatics.vocab.Undefined;
import edu.mayo.pipes.history.FindAndReplaceHPipe;
import edu.mayo.pipes.history.HCutPipe;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.history.HistoryOutPipe;
import edu.mayo.pipes.util.GenomicObjectUtils;
import edu.mayo.util.HGVS;

/**
 *
 * @author Surendra Konathala
 */
public class CosmicPublisher {
    public static void usage(){
        System.out.println("usage: CosmicPublisher <rawDataFile> <catalogOutputDir>");
    }
    
    public static void main(String[] args) {	 
        CosmicPublisher publisher = new CosmicPublisher();      
        //publisher.publish("/data/cosmic/v62/CosmicCompleteExport_v62_291112.tsv.gz", "/data/catalogs/cosmic/v62");
        //publisher.publish("C:\\mayo\\bior\\cosmic\\CosmicCompleteExport_v62_291112.tsv.gz", "C:\\temp");
                
        if(args.length >= 1){ 
            publisher.publish(args[0], args[1] + "/scratch/");
        }else{
            usage();
            System.exit(1);
        }
    }     
    
     
    /**
     *
     * 
     */
    public void publish(String rawDataFile, String outputDir) {
        final String catalogFile = "cosmic.tsv";
        
        String[] header = getHeader(rawDataFile);
        List<String> processedHeader = postProcessHeader(header);
        System.out.println("Processed Header:\n"+Arrays.asList(processedHeader));
       
        System.out.println("Started loading Cosmic at: " + new Timestamp(new Date().getTime()));
        
        //String outfile = outputDir + "\\" + catalogFile; 
        String outfile = outputDir + catalogFile;        
        System.out.println("Outputing File to: " + outputDir + "cosmic.tsv");
                
        //processCosmicFile(rawDataFile, processedHeader, new PrintPipe());
        processCosmicFile(rawDataFile, processedHeader, new WritePipe(outfile));
        
        System.out.println("COMPLETED loading Cosmic at: " + new Timestamp(new Date().getTime()));
    }    
    
    /**
     * Returns the HEADER columns from the raw data file
     */
     private String[] getHeader(String rawDataFile) {    	
     	String header="";
     	
         Pipe p = new Pipeline(new HeadPipe(1, new CatGZPipe("gzip")));
         p.setStarts(Arrays.asList(rawDataFile));
         for(int i=0;p.hasNext();i++) {
         	header = (String) p.next();
         	//System.out.println(header);        	
         }
         
         return header.split("\t");
     }
     
     /**
      *  
      * Processes the header columns, replaces spaces within a column name to "_".
      * "Gene name" to "Gene_name"
      */
     private List<String> postProcessHeader(String[] header) {
 		List<String> processedHeader = new ArrayList<String>();
 		
 		FindAndReplaceHPipe replaceChr = new FindAndReplaceHPipe(1, " ", "_");
 		
 		Pipe p = new Pipeline(new HistoryInPipe(), replaceChr, new HistoryOutPipe());
 		p.setStarts(Arrays.asList(header));
         String result = "";
         for(int i=0;p.hasNext();i++){
             result = (String) p.next();
             
             if (i>0) { //the first row has junk like #UNKNOWN.. so remove 
             	processedHeader.add(result);
             }
         }
         
 		return processedHeader;
 	}    

   
    /**
     *
     * 
     */
    private void processCosmicFile(String file, List<String> headerColumns, Pipe load) {

    	//String[] headerArray = headerColumns.toArray(new String[headerColumns.size()]);
    	    	
    	Injector[] injectors = new Injector[33];
    	
    	for(int i=0;i<headerColumns.size();i++) {    		
    		injectors[i] =  new ColumnInjector(i+1, headerColumns.get(i), JsonType.STRING);
    	}
       	//Add CoreAttributes    	
    	injectors[25] = new ColumnInjector(26, CoreAttributes._type.toString(), JsonType.STRING);                
    	injectors[26] = new ColumnInjector(27, CoreAttributes._landmark.toString(), JsonType.STRING);
    	injectors[27] = new ColumnInjector(28, CoreAttributes._refAllele.toString(), JsonType.STRING);
    	injectors[28] = new ColumnArrayInjector(29, CoreAttributes._altAlleles.toString(), JsonType.STRING, ",");
    	injectors[29] = new ColumnInjector(30, CoreAttributes._minBP.toString(), JsonType.NUMBER);
    	injectors[30] = new ColumnInjector(31, CoreAttributes._maxBP.toString(), JsonType.NUMBER);
    	injectors[31] = new ColumnInjector(32, CoreAttributes._strand.toString(), JsonType.STRING);
    	injectors[32] = new LiteralInjector(CoreAttributes._id.toString(),".",JsonType.STRING);
    	
        InjectIntoJsonPipe injectCosmicDataAsJson = new InjectIntoJsonPipe(true, injectors);
        
        int[] cut = new int[] {3,4,5,6,7,8,9,10,11,12,14,15,16,19,20,21,22,23,24,25,26,28,29,32};
        
        Pipe<History,History> transform = new TransformFunctionPipe<History,History>( new CosmicTransformPipe() );
        
        Pipe p = new Pipeline(new CatGZPipe("gzip"),
        						new HeaderPipe(1),        						
        						new HistoryInPipe(),        						
        						transform,  
        						injectCosmicDataAsJson,
        						new HCutPipe(false, cut),
        						new MergePipe("\t", true),
        						load
        );
        p.setStarts(Arrays.asList(file));
        for(int i=0; p.hasNext(); i++){
            //System.out.println("Val="+i);
            p.next();                     
            //if(i>500) break;
        }
        
    }
    
    public CosmicTransformPipe getCosmicTransformPipe() {
    	CosmicTransformPipe ctp = new CosmicTransformPipe();
    	return ctp;
    }

	/**
	 * 
	 * @author Surendra Konathala
	 *
	 */
	public class CosmicTransformPipe implements PipeFunction<History,History> {

		/**
		 * Columns in raw data file:
			1: Gene name,
			2: Accession Number, 
			3: HGNC ID, 
			4: Sample name, 
			5: ID_sample, 
			ID_tumour, 
			Primary site, 
			Site subtype, 
			Primary histology, 
			Histology subtype, 
			Genome-wide screen, 
			Mutation ID, 
			* 13: Mutation CDS, [has value like c.35C>G. Used to get REF & ALT allele]
			Mutation AA, 
			Mutation Description, 
			Mutation zygosity, 
			Mutation NCBI36 genome position, 
			Mutation NCBI36 strand, 
			* 19: Mutation GRCh37 genome position, [value: 12:12345:12346.. get chr, and positions]
			* 20: Mutation GRCh37 strand, 
			Mutation somatic status, 
			Pubmed_PMID, 
			Sample source, 
			Tumour origin, 
			25: Comments
		 */
			
		String rawData = "";
		String temp = "";
		String chr = "";
		String minBp = "";
		String maxBp = "";
		String ref = "";
		String[] alt;
		String strand = "";		
		
		@Override
		public History compute(History history) {
			
			/**
			 * Make sure the history-in pipe always has the exact number of 
			 * columns the raw data file. This is because, this history-in
			 * pipe is used in "inject2json" which expects all columns to be there.
			 * 
			 * Also the reason this is being checked here is that 'Cosmic RawDataFile'
			 * has some data on some columns missing. If data is missing, an empty string is
			 * added in the file.. but if the last column "Comments" is missing
			 * nothing is added and the history-in that is being returned is sent without the last column
			 * and this fails at "inject2json" pipe since it expects all columns to be there.		 
			 */
			if (history.size()<25) {
				int missingColumns = 25 - history.size();
				for (int i=0;i<missingColumns;i++) {
					history.add("");
				}
			}
			
			//chr = Undefined.UNKNOWN.toString(); // DEFAULT
			chr = "";
			minBp = ""; // DEFAULT
			maxBp = ""; // DEFAULT
			ref = ""; // DEFAULT
			alt = new String[1]; // DEFAULT
			strand = ""; // DEFAULT
			
			//_type
            history.add(Type.VARIANT.toString());
            
            //Compute REF, ALT
            this.computeAlleles(history);

            //Compute CHR, MINBP, MAXBP
            this.computeGenomePostion(history);
            
            //Compute Strand
            this.computeStrand(history);

            //_landmark            
            history.add(this.chr);
            
            //_refAllele
            history.add(this.ref);
            
            //_altAllele
            //history.add(this.alt);
            history.add(StringUtils.join(this.alt, ","));

            //_minBP
            history.add(this.minBp);

            //_maxBP
            history.add(this.maxBp);            
                       
            //_strand
            history.add(this.strand);
            
            return history;
		}

		 // Data for a alleles is in Col 13 and is like "c.35G>A"
        private void computeAlleles(History history) {
                if (history.size()>=12) {
                        if (history.get(12)!=null && !history.get(12).equals("")) {
                                this.rawData = history.get(12);
                                //USe this generic class from google-code "snp-normaliser" that parses HGVS nomenclature mutation like "c.123G>A"
                                if (this.rawData.contains("?")) {
                                        //System.out.println("BAD");
                                        //some data in cosmis raw file has invalid CDSMutations like "c.?", this avoid them
                                } else {
                                        HGVS hgvs = new HGVS(this.rawData);
                                        if (hgvs.getWildtype()!=null){
                                                this.ref = hgvs.getWildtype();
                                        }

                                        if (hgvs.getMutation()!=null){
                                                this.alt[0] = hgvs.getMutation();
                                        }
                                }
                        }
                }
        }


		// Data for a genome-postion is in Col 19 and is like "10:1234-1235" chr:minbp-maxbp
		private void computeGenomePostion(History history) {
			//System.out.println(history.size());
			if (history.size()>=18) {
				//System.out.println(Arrays.asList(history));
				if (history.get(18)!=null && !history.get(18).equals("")) {				
					
					this.rawData = history.get(18);
					
					//Chr
					this.temp = rawData.substring(0, rawData.indexOf(":"));					
					this.chr = GenomicObjectUtils.computechr(this.temp);					
	
					//minBP
					this.minBp = this.rawData.substring(this.rawData.indexOf(":")+1, this.rawData.indexOf("-"));
					
					//maxBP
					this.maxBp = this.rawData.substring(this.rawData.indexOf("-")+1, this.rawData.length());
				}
			} 			
		}

		// Data for strand is in Col 18 and is like "-" or "+"
		private void computeStrand(History history) {
			if (history.size()>=19) {
				if (history.get(19)!=null && !history.get(19).equals("")) {			
					this.strand = Character.toString(GenomicObjectUtils.getStrand(history.get(19)));
				}
			}			
		}
		
	}
}