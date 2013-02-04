package edu.mayo.bior.publishers.BGIDanish;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.transform.TransformFunctionPipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.MergePipe;
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
import edu.mayo.pipes.util.GenomicObjectUtils;

/**
 * @author m102417
 */
public class BGIPublisher {
    public static void usage(){
        System.out.println("usage: BGIPublisher <rawDataFile> <rawOutputFile>");
        System.out.println("<rawDataFile> should be of the following format:");
        System.out.println("###chr	.	.	minBP	maxBP	majIdx	minIdx	As	Cs	Gs	Ts	Freqs	minBpOrig	RefAllele	isInDbSNP");
        System.out.println("chr1	.	.	69428	69428	3	2	0	7	86	1871	\"0.073021\"	59291	T	0");
    }
    
    
    public static void main(String[] args) {	 
        BGIPublisher publisher = new BGIPublisher();
        // Ex:  publisher.publish("/data/BGI/hg19/LuCAMP_200exomeFinal_hg19.txt", "/tmp/LuCAMP_200exomeFinal_hg19.tsv");
        if( args.length != 2 ) {
            usage();
            System.exit(1);
        }

        String infile = args[0];
        String outfile = args[1];
        System.out.println("Input File:  " + infile);
        System.out.println("Output File: " + outfile);  
        publisher.publish(infile, outfile);
    } 
    
    public void publish(String rawDataFileFullpath, String outfile) {
        double start = System.currentTimeMillis();
        System.out.println("Started loading BGI at: " + new Timestamp(new Date().getTime()));
        Pipe<History,History> xformPipe = new TransformFunctionPipe<History,History>( new BGIPipe() );
        InjectIntoJsonPipe inject = new InjectIntoJsonPipe(true, new Injector[] {
            new ColumnInjector(1, "chromosomeID", JsonType.STRING),
            new ColumnInjector(4, "genomic_position", JsonType.STRING),
            new ColumnInjector(13,"genomic_position_original", JsonType.STRING),
            new ColumnInjector(6, "index_of_major_allele", JsonType.NUMBER),
            new ColumnInjector(7, "index_of_minor_allele", JsonType.NUMBER),
            new ColumnInjector(8, "number_A", JsonType.NUMBER),
            new ColumnInjector(9, "number_C", JsonType.NUMBER), 
            new ColumnInjector(10,"number_G", JsonType.NUMBER),
            new ColumnInjector(11,"number_T", JsonType.NUMBER),
            new ColumnInjector(12, "estimatedMAF", JsonType.STRING),
            new ColumnInjector(19, "isInDbSNP", JsonType.STRING),
            new ColumnInjector(17, CoreAttributes._landmark.toString(), JsonType.STRING),
            new ColumnInjector(15, CoreAttributes._refAllele.toString(), JsonType.STRING),
            new ColumnArrayInjector(18, CoreAttributes._altAlleles.toString(), JsonType.STRING, ","),
            //some checking on hg19 shows it to be one based not zero based
            new ColumnInjector(4, CoreAttributes._minBP.toString(), JsonType.NUMBER),
            new ColumnInjector(5, CoreAttributes._maxBP.toString(), JsonType.NUMBER),
        	new LiteralInjector(CoreAttributes._type.toString(), Type.VARIANT.toString(), JsonType.STRING),
            new LiteralInjector(CoreAttributes._id.toString(),".",JsonType.STRING)
        });

        Pipe p = new Pipeline(new CatPipe(),
                             new HistoryInPipe(),
                             xformPipe,
                             inject,
                             new HCutPipe(false, new int[] {2,3,6,7,8,9,10,11,12,13,14,15,16,17,18,19} ), 
                             new MergePipe("\t"),
                         	 // Write to file: Don't append to file;  Add newlines to each line
                             new WritePipe(outfile, false, true),
                             //new PrintPipe()
                );
        p.setStarts(Arrays.asList(rawDataFileFullpath));
        while(p.hasNext()) {
            p.next();
        }
        double end = System.currentTimeMillis();
        System.out.println("Total runtime: " + (end-start)/1000.0 + " sec");
    }
    
    public class BGIPipe implements PipeFunction<History,History>{
        @Override
        /** Add on extra columns that we will then inject into the JSON object */
        public History compute(History h) {
            //_landmark
            h.add(  GenomicObjectUtils.computechr(h.get(0) ));
            
            //_altAllele
            String ref = h.get(14);
            String altsJson = getAltAllelesJson(ref, Integer.parseInt(h.get(7)), Integer.parseInt(h.get(8)), Integer.parseInt(h.get(9)), Integer.parseInt(h.get(10)));
            h.add(altsJson);
            
            // isInDbSNP - convert to true/false (instead of 0/1)
            String isInDbSNP0or1 = h.get(15);
            h.add("" + "1".equals(isInDbSNP0or1));
            
            // Throw an exception if the minBP column from the original maf file (col 12) 
            // does NOT match the minBP column from the original genotype file (col 13)
            if( ! h.get(12).equals(h.get(13)) )
            	throw new IllegalArgumentException("The minBP from the maf does NOT match the minBP from the genotype file!");
            
            return h;
        }

    	/** Convert index to base-pair:  0=A, 1=C, 2=G, 3=T */
    	private String decode(String s){
            if(s.endsWith("0")){
                return "A";
            }else if(s.endsWith("1")){
                return "C";
            }else if(s.endsWith("2")){
                return "G";
            }else if(s.endsWith("3")){
                return "T";
            }
            return null;
        }

        /** Get the alt alleles as a comma-separated string.  
         *  Check the # of As, Cs, Gs, Ts - if any are non-zero AND are NOT the ref allele, add them to list */ 
        private String getAltAllelesJson(String refAllele, int numAs, int numCs, int numGs, int numTs) {
        	String alts = "";
        	
        	if( ! "A".equals(refAllele) && numAs > 0 )
        		alts += "A,";
        	
        	if( ! "C".equals(refAllele) && numCs > 0 )
        		alts += "C,";
        	
        	if( ! "G".equals(refAllele) && numGs > 0 )
        		alts += "G,";

        	if( ! "T".equals(refAllele) && numTs > 0 )
        		alts += "T";
        		
        	if(alts.endsWith(","))
        		alts = alts.substring(0,alts.length()-1);
        	return  alts;
        }
    }
    
}
