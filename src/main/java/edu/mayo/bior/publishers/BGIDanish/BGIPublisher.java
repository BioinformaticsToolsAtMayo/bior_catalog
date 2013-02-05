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
 * @author Daniel Quest, Michael Meiners
 */
public class BGIPublisher {
    public static void usage(){
        System.out.println("usage: BGIPublisher <rawDataFile> <rawOutputFile>");
        System.out.println("<rawDataFile> should be of the following format:");
        System.out.println("###chr	.	.	minBP	maxBP	majIdx	minIdx	As	Cs	Gs	Ts	Freqs	minBpOrigMaf	minBpOrigGenotype	RefAllele	isInDbSNP");
        System.out.println("chr1	.	.	69428	69428	3	2	0	7	86	1871	\"0.073021\"	59291	T	0");
    }
    
    int totalLines = 0;
    int numRefNotMajor = 0;
    int numRefNotMajorOrMinor = 0;
    int numRefNotMajorOrMinorAndMajorIsNotMinor = 0;
    
    private class Col {
    	// Columns passed in:
    	// NOTE: The first column (ChromLong) will be modified and changed to a short representation ("chr17" -> "17")
    	public static final int ChromShort 			= 0;
    	public static final int Dot1 				= 1;
    	public static final int Dot2 				= 2;
    	public static final int MinBPAfterLiftOver 	= 3;
    	public static final int MaxBPAfterLiftOver 	= 4;
    	public static final int MajorIndex			= 5;
    	public static final int MinorIndex			= 6;
    	public static final int ACount				= 7;
    	public static final int CCount				= 8;
    	public static final int GCount				= 9;
    	public static final int TCount				= 10;
    	public static final int EstimatedMinorAlleleFrequency = 11;
    	public static final int MinBpOrigMaf		= 12;
    	public static final int MinBpOrigGen		= 13;
    	public static final int RefAllele			= 14;
    	public static final int IsInDbSnp			= 15;
    	// Columns that will be added:
    	public static final int ChromLongOrig		= 16;
    	public static final int Alts				= 17;
    	public static final int MajorAllele			= 18;
    	public static final int MinorAllele			= 19;
    	public static final int EstimatedMajorAlleleFrequency 	= 20;
    	public static final int CalculatedMinorAlleleFrequency 	= 21;
    	public static final int CalculatedMajorAlleleFrequency 	= 22;
    	public static final int BgiJson				= 23;
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
        	// Columns are 1-based (so adding the 1)
            new ColumnInjector(1+Col.ChromLongOrig,	"chromosome_id", 				JsonType.STRING),
            new ColumnInjector(1+Col.MinBpOrigMaf,	"genomic_position", 			JsonType.NUMBER),
            new ColumnInjector(1+Col.MinBPAfterLiftOver,"genomic_position_after_liftOver", JsonType.NUMBER),
            new ColumnInjector(1+Col.MajorIndex, 	"index_of_major_allele", 		JsonType.NUMBER),
            new ColumnInjector(1+Col.MinorIndex,	"index_of_minor_allele", 		JsonType.NUMBER),
            new ColumnInjector(1+Col.ACount,		"number_A", 					JsonType.NUMBER),
            new ColumnInjector(1+Col.CCount, 		"number_C", 					JsonType.NUMBER), 
            new ColumnInjector(1+Col.GCount,		"number_G", 					JsonType.NUMBER),
            new ColumnInjector(1+Col.TCount,		"number_T", 					JsonType.NUMBER),
            new ColumnInjector(1+Col.MajorAllele,	"major_allele",					JsonType.STRING),
            new ColumnInjector(1+Col.MinorAllele,	"minor_allele", 				JsonType.STRING),
            new ColumnInjector(1+Col.EstimatedMinorAlleleFrequency,	"estimated_minor_allele_freq", JsonType.NUMBER),
            new ColumnInjector(1+Col.EstimatedMajorAlleleFrequency,	"estimated_major_allele_freq", JsonType.NUMBER),
            new ColumnInjector(1+Col.CalculatedMinorAlleleFrequency,"calculated_minor_allele_freq", JsonType.NUMBER),
            new ColumnInjector(1+Col.CalculatedMajorAlleleFrequency,"calculated_major_allele_freq", JsonType.NUMBER),
            new ColumnInjector(1+Col.IsInDbSnp, 	"is_in_dbSNP", 					JsonType.NUMBER),
            new ColumnInjector(1+Col.ChromShort, 	CoreAttributes._landmark.toString(), JsonType.STRING),
            new ColumnInjector(1+Col.RefAllele,		CoreAttributes._refAllele.toString(), JsonType.STRING),
            new ColumnArrayInjector(1+Col.Alts,		CoreAttributes._altAlleles.toString(), JsonType.STRING, ","),
            //some checking on hg19 shows it to be one based not zero based
            new ColumnInjector(1+Col.MinBPAfterLiftOver, CoreAttributes._minBP.toString(), JsonType.NUMBER),
            new ColumnInjector(1+Col.MaxBPAfterLiftOver, CoreAttributes._maxBP.toString(), JsonType.NUMBER),
        	new LiteralInjector(CoreAttributes._type.toString(), Type.VARIANT.toString(), JsonType.STRING),
            new LiteralInjector(CoreAttributes._id.toString(),".",JsonType.STRING)
        });

        Pipe p = new Pipeline(new CatPipe(),
                             new HistoryInPipe(),
                             xformPipe,
                             inject,
                             // Columns to remove - 1 based
                             new HCutPipe(false, new int[] {
                            		 1+Col.Dot1,
                            		 1+Col.Dot2,
                            		 1+Col.MajorIndex,
                            		 1+Col.MinorIndex,
                            		 1+Col.ACount,
                            		 1+Col.CCount,
                            		 1+Col.GCount,
                            		 1+Col.TCount,
                            		 1+Col.EstimatedMinorAlleleFrequency,
                            		 1+Col.MinBpOrigMaf,
                            		 1+Col.MinBpOrigGen,
                            		 1+Col.RefAllele,
                            		 1+Col.IsInDbSnp,
                            		 1+Col.ChromLongOrig,
                            		 1+Col.Alts,
                            		 1+Col.MajorAllele,
                            		 1+Col.MinorAllele,
                            		 1+Col.EstimatedMajorAlleleFrequency,
                            		 1+Col.CalculatedMinorAlleleFrequency,
                            		 1+Col.CalculatedMajorAlleleFrequency
                             } ), 
                             new MergePipe("\t"),
                         	 // Write to file: Don't append to file;  Add newlines to each line
                             new WritePipe(outfile, false, true)
                             //new PrintPipe()
                );
        p.setStarts(Arrays.asList(rawDataFileFullpath));
        while(p.hasNext()) {
            p.next();
        }
        System.out.println("Total lines: " + totalLines);
        System.out.println("Num occurrences where ref is not the major allele: " + numRefNotMajor);
        System.out.println("Num occurrences where ref is neither the major nor minor allele: " + numRefNotMajorOrMinor);
        System.out.println("Num occurrences where ref is neither the major nor minor allele, and the major is not the minor: " + numRefNotMajorOrMinorAndMajorIsNotMinor);
        double end = System.currentTimeMillis();
        System.out.println("Total runtime: " + (end-start)/1000.0 + " sec");
    }
    
    public class BGIPipe implements PipeFunction<History,History>{
        @Override
        /** Add on extra columns that we will then inject into the JSON object 
         *  NOTE:  History indexes are 0-based */
        public History compute(History h) {
            //_landmark
        	// NOTE: First column starts out as a long chrom, but we convert it to short
        	String chromLong  = h.get(Col.ChromShort);
        	String chromShort = GenomicObjectUtils.computechr(chromLong);
            h.add(chromLong);
            // Change the first column to be the short version ("17" vs "chr17")
            h.set(Col.ChromShort, chromShort);
            
            //_altAllele
            String ref = h.get(Col.RefAllele);
            String major = decode(h.get(Col.MajorIndex));
            String minor = decode(h.get(Col.MinorIndex));
            //String altsJson = getAltAllelesJson(ref, Integer.parseInt(h.get(7)), Integer.parseInt(h.get(8)), Integer.parseInt(h.get(9)), Integer.parseInt(h.get(10)));
            String altsJson = getAltAllelesFromMajorMinor(ref, major, minor);
            h.add(altsJson);
            
            // Add Major and Minor Alleles
            h.add(major);
            h.add(minor);
            
            // Create Estimated MAJOR Allele Frequency
            double estMinorAlleleFreq = Double.parseDouble(h.get(Col.EstimatedMinorAlleleFrequency));
            double estMajorAlleleFreq = 1 - estMinorAlleleFreq;
            h.add("" + estMajorAlleleFreq);
            
            // Compute CALCULATED Minor Allele Frequency  and CALCULATED Major Allele Frequency
            double[] countsACGT = new double[] {
            		Double.parseDouble(h.get(Col.ACount)),
            		Double.parseDouble(h.get(Col.CCount)),
            		Double.parseDouble(h.get(Col.GCount)),
            		Double.parseDouble(h.get(Col.TCount))
            };
            int majorIdx = Integer.parseInt(h.get(Col.MajorIndex));
            int minorIdx = Integer.parseInt(h.get(Col.MinorIndex));
            double sumCounts = countsACGT[0] + countsACGT[1] + countsACGT[2] + countsACGT[3];
            double calcMinorAlleleFreq = countsACGT[minorIdx] / sumCounts;
            double calcMajorAlleleFreq = countsACGT[majorIdx] / sumCounts;
            h.add("" + calcMinorAlleleFreq);
            h.add("" + calcMajorAlleleFreq);
            
            // Throw an exception if the minBP column from the original maf file (col 12) 
            // does NOT match the minBP column from the original genotype file (col 13)
            if( ! h.get(Col.MinBpOrigMaf).equals(h.get(Col.MinBpOrigGen)) )
            	throw new IllegalArgumentException("The minBP from the maf does NOT match the minBP from the genotype file!");
            
            if( ! ref.equals(major) ) {
            	numRefNotMajor++;
            }
            if( ! ref.equals(major) && ! ref.equals(minor)) {
            	//System.out.println(h);
            	numRefNotMajorOrMinor++;
            }
            if( ! ref.equals(major) && ! ref.equals(minor) && ! major.equals(minor)) {
            	//System.out.println(h);
            	numRefNotMajorOrMinorAndMajorIsNotMinor++;
            }
            
            totalLines++;

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
    	 *  Check the ref against the major and minor alleles.  
    	 *  If either is different from the ref, add it.
    	 *  ------------
    	 *  Some stats from full BGI file:
    	 *  Total lines: 121858
    	 *  Num occurrences where ref is not the major allele: 15750
    	 *  Num occurrences where ref is neither the major nor minor allele 804
    	 *  Num occurrences where ref is neither the major nor minor allele, and the major is not the minor 701
    	 */
    	private String getAltAllelesFromMajorMinor(String refAllele, String majorAllele, String minorAllele) {
    		String alts = "";
    		if( ! refAllele.equals(majorAllele) )
    			alts += majorAllele + ",";
    		if( ! refAllele.equals(minorAllele) )
    			alts += minorAllele;
    		
    		if(alts.endsWith(",")) 
    			alts = alts.substring(0, alts.length()-1);
    		return alts;
    	}
    	
        /** Get the alt alleles as a comma-separated string.  
         *  Check the # of As, Cs, Gs, Ts - if any are non-zero AND are NOT the ref allele, add them to list */ 
//        private String getAltAllelesFromCounts(String refAllele, int numAs, int numCs, int numGs, int numTs) {
//        	String alts = "";
//        	
//        	if( ! "A".equals(refAllele) && numAs > 0 )
//        		alts += "A,";
//        	
//        	if( ! "C".equals(refAllele) && numCs > 0 )
//        		alts += "C,";
//        	
//        	if( ! "G".equals(refAllele) && numGs > 0 )
//        		alts += "G,";
//
//        	if( ! "T".equals(refAllele) && numTs > 0 )
//        		alts += "T";
//        		
//        	if(alts.endsWith(","))
//        		alts = alts.substring(0,alts.length()-1);
//        	return  alts;
//        }
    }
    
}
