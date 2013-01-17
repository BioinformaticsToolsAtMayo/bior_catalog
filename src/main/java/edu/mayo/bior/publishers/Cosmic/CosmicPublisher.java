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
 * @author m089716
 */
public class CosmicPublisher {
        public static void usage(){
        System.out.println("usage: CosmicPublisher <rawDataFile> <catalogOutputDir>");
    }
    
    public static void main(String[] args) {	 
        CosmicPublisher publisher = new CosmicPublisher();
        publisher.publish("C:\\mayo\\bior\\cosmic\\CosmicCompleteExport_v62_291112.tsv.gz", "C:\\temp");
        //publisher.publish("C:\\mayo\\bior\\cosmic\\cosmic_mart_export.txt", "C:\\temp");        
    }     
    
   /**
    * Returns the HEADER columns from the raw data file
    */
    public String[] getHeader(String rawDataFile) {    	
    	String header="";
    	
        Pipe p = new Pipeline(new HeadPipe(1, new CatGZPipe("gzip")));
        p.setStarts(Arrays.asList(rawDataFile));
        for(int i=0;p.hasNext();i++) {
        	header = (String) p.next();
        	//System.out.println(header);        	
        }
        
        //System.out.println(header);
    	//String[] val = header.split("\t");
        //System.out.println(Arrays.asList(val));
        
        return header.split("\t");
    }
    
    /**
     * TODO
     * 1. get header [done]
     * 2. process each header element with "_" [done]
     * 3. get value from col - 'CDS Mutation Syntax' which is like "c.35G>A",
     *    parse it to retrieve REF and ALT
     * 4. load them like in hapmappublish phase 1
     * 4. parse the file and for each json value(header column add the values from raw file
     * like gene_name="39443", Accesssion_number="ENSSSS" etc
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
        
        //converting array-list to string[]
        String[] strarray = processedHeader.toArray(new String[processedHeader.size()]);
        
        //System.out.println("PH:\n\n"+Arrays.asList(strarray));
        
		//return strarray;
		return processedHeader;
	}    
    
    /**
     *
     * 
     */
    public void publish(String rawDataFile, String outputDir) {
        final String catalogFile = "cosmic.tsv";
        
        String[] header = getHeader(rawDataFile);
        //String[] processedHeader = postProcessHeader(header);
        List<String> processedHeader = postProcessHeader(header);
        System.out.println("Processed Header:\n"+Arrays.asList(processedHeader));
       
        System.out.println("Started loading HapMap at: " + new Timestamp(new Date().getTime()));
        
        //String outfile = outputDir + "/" + catalogFile; 
        String outfile = outputDir + "\\" + catalogFile;        
        System.out.println("Outputing File to: " + outfile);
        
        processCosmicFile(rawDataFile, processedHeader, new WritePipe(outfile));
    }    
    
   
    /**
     *
     * 
     */
    private void processCosmicFile(String file, List<String> headerColumns, Pipe load) {

        Injector[] injectors = new Injector[]
        		{
        			new LiteralInjector(CoreAttributes._type.toString(), Type.VARIANT.toString(), JsonType.STRING),
        			new ColumnInjector     (1, CoreAttributes._landmark.toString(),   JsonType.STRING),
        			new ColumnInjector     (2, CoreAttributes._minBP.toString(),      JsonType.NUMBER),
        			new ColumnInjector     (3, CoreAttributes._maxBP.toString(),      JsonType.NUMBER),
        			new ColumnInjector     (4, CoreAttributes._strand.toString(),     JsonType.STRING),
        			new ColumnInjector     (5, CoreAttributes._refAllele.toString(),  JsonType.STRING),
        			new ColumnArrayInjector(6, CoreAttributes._altAlleles.toString(), JsonType.STRING, ","),
        			new ColumnInjector     (7, CoreAttributes._id.toString(),         JsonType.STRING)        			        			        			
        		};
        
        InjectIntoJsonPipe injectCosmicDataAsJson = new InjectIntoJsonPipe(8, injectors);
        
        int[] cut = new int[] {3,4,5,6,7,8,9,10,11,12,14,15,16,19,20,21,22,23,24,25,26,28,29,32};
        
        Pipe<History,History> transform = new TransformFunctionPipe<History,History>( new CosmicTransformPipe() );

        HistoryInPipe hPipe = new HistoryInPipe(25);
        
        Pipe p = new Pipeline(new CatGZPipe("gzip"),
        						new HeaderPipe(1),        						
        						hPipe,        						
        						transform,  
        						injectCosmicDataAsJson,
        						new HCutPipe(false, cut),
        						new MergePipe("\t", true),
        						load
        );
        p.setStarts(Arrays.asList(file));
        for(int i=0; p.hasNext(); i++){
            //p.next();
        	System.out.println("Val="+i+" | "+p.next());
         
            //if(i>155) break;
        }
    }

	/**
	 * 
	 * @author m089716
	 *
	 */
	public class CosmicTransformPipe implements PipeFunction<History,History> {

		/**
		 * Columns in raw data file:
			Gene name,
			Accession Number, 
			HGNC ID, 
			Sample name, 
			ID_sample, 
			ID_tumour, 
			Primary site, 
			Site subtype, 
			Primary histology, 
			Histology subtype, 
			Genome-wide screen, 
			Mutation ID, 
			* Col 13: Mutation CDS, [has value like c.35C>G. Used to get REF & ALT allele]
			Mutation AA, 
			Mutation Description, 
			Mutation zygosity, 
			* Col 17: Mutation NCBI36 genome position, [value: 12:12345:12346.. get chr, and positions]
			* Col 18: Mutation NCBI36 strand, 
			Mutation GRCh37 genome position, [col 19]
			Mutation GRCh37 strand, 
			Mutation somatic status, 
			Pubmed_PMID, 
			Sample source, 
			Tumour origin, 
			Comments
		 */
			
		String rawData = "";
		String temp = "";
		String chr = "";
		String minBp = "";
		String maxBp = "";
		String ref = "";
		String alt = "";
		String strand = "";		
		
		@Override
		public History compute(History history) {
			
			chr = Undefined.UNKNOWN.toString(); // DEFAULT
			minBp = "0"; // DEFAULT
			maxBp = "0"; // DEFAULT
			ref = "N"; // DEFAULT
			alt = "N"; // DEFAULT
			strand = "."; // DEFAULT
			
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
            
            //_minBP
            history.add(this.minBp);

            //_maxBP
            history.add(this.maxBp);
            
            //_refAllele
            history.add(this.ref);
            
            //_altAllele
            history.add(this.alt);
            
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
					HGVS hgvs = new HGVS(this.rawData);				
					this.ref = hgvs.getWildtype();
					this.alt = hgvs.getMutation();
				}
			}
		}	

		// Data for a genome-postion is in Col 17 and is like "10:1234-1235" chr:minbp-maxbp
		private void computeGenomePostion(History history) {
			if (history.size()>=16) {
				if (history.get(16)!=null && !history.get(16).equals("")) {				
					//System.out.println(Arrays.asList(history));
					this.rawData = history.get(16);
					
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
			if (history.size()>=17) {
				if (history.get(17)!=null && !history.get(17).equals("")) {			
					this.strand = Character.toString(GenomicObjectUtils.getStrand(history.get(17)));
				}
			}			
		}
		
	}
}