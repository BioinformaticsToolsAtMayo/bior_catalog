/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.NCBIGene;


import java.io.FileNotFoundException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.DrainPipe;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.WritePipe;
import edu.mayo.pipes.JSON.BioJavaRichSequence2JSON;
import edu.mayo.pipes.JSON.SimpleDrillPipe;
import edu.mayo.pipes.UNIX.GrepPipe;
import edu.mayo.pipes.UNIX.LSPipe;
import edu.mayo.pipes.bioinformatics.GenbankPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;

/**
 *  This class creates all the catalogs needed for NCBIGene.
 *  It assumes you have already called:
 *      bior_catalog/downloaders/downloadGenes.bash
 *      bior_catalog/downloaders/uncompressGenes.bash
 * 
 * those scripts will download the raw information, and uncompress/grep it into something
 * more usable.  Assume that this usable stuff is in:
 * 
 * $BIOR_CATALOG_HOME/NCBIGene/version/scratch
 * 
 * @author m102417
 */
public class NCBIGenePublisher {
    
	private static Logger sLogger = Logger.getLogger(NCBIGenePublisher.class);
	
    public static void usage(){
        System.out.println("usage: NCBIGenePublisher <rawDataDir> <catalogOutputDir>");
        System.out.println("Where <rawDataDir> is something like: ");
        System.out.println("  /data5/bsi/refdata-new/ncbi_genome/human/downloaded/latest/2014_02_04/Assembled_chromosomes/gbs");
        System.out.println("and contains files (one per chromosome) like:");
        System.out.println("  hs_ref_GRCh37.p13_chr11.gbs.gz");
        System.out.println("Example data within this file starts with a lot of human-readable text, but then after this info header, contains a lot of repeating data for 'gene', 'CDS', 'mRNA', etc");
        System.out.println("Example public URL where the data may come from:");
        System.out.println("  ftp://ftp.ncbi.nlm.nih.gov/genomes/H_sapiens/");
    }
    
    public static void main(String[] args) {	 
        NCBIGenePublisher publisher = new NCBIGenePublisher();
        System.out.println(args.length);
        if(args.length >= 1){
            publisher.publish(args[0], args[1]);
        }else{
            usage();
            System.exit(1);
        }
    } 

    public void publish(String rawDataDir, String outputDir) {
        final String geneCatalogFile = "genes.tsv";

        double start = System.currentTimeMillis();
    	System.out.println("Started loading NCBIGenes at: " + new Timestamp(new Date().getTime()));
        String outfile = outputDir + "/" + geneCatalogFile;
        System.out.println("Outputing File to: " + outfile);

        try {
            System.out.println("Parsing Genes from: " + rawDataDir); //chrDir);            
            
            List<String> files = getGeneGzips(rawDataDir);

            for(String filename : files) {
                System.out.println("Processing File: " + filename);
                String chrstr = getChromFromFilename(filename);
                processGenes(rawDataDir + "/" + filename, chrstr, outfile);
            }
        } catch (Exception ex) {
            sLogger.error("Failed to publish NCBIGene catalog!", ex);
            ex.printStackTrace();
        }        
        System.out.println("Completed loading NCBIGenes at: " + new Timestamp(new Date().getTime()));
        double end = System.currentTimeMillis();
        System.out.println("Runtime: " + (end-start)/1000.0);
	}

    /** From a filename such as "hs_ref_GRCh37.p13_chr14.gbs.gz", get the chromosome (ex: "14") */
	private String getChromFromFilename(String filename) {
		int idx1 = filename.indexOf("_chr") + 4;
		int idx2   = filename.indexOf(".gbs.gz");
		String chrstr = filename.substring(idx1, idx2).toUpperCase();
		//String c = GenomicObjectUtils.computechr(chrstr); 
		return chrstr;
	}

	/** Match filenames:  hs_ref_GRCh*.gbs.gz"
	 	Ex:  hs_ref_GRCh37.p13_chr14.gbs.gz  	*/
	private List<String> getGeneGzips(String rawDataDir)  throws FileNotFoundException {
		final String FILE_PATTERN = "hs_ref_GRCh\\S+_chr\\S+\\.gbs\\.gz";
		Pipeline<String,String> p = new Pipeline(new Pipe[] {new LSPipe(false), new GrepPipe(FILE_PATTERN)});
		p.setStarts(Arrays.asList(new String[] {rawDataDir}));
		List<String> files = new ArrayList<String>();
		while(p.hasNext()) {
			files.add(p.next());
		}
		
		// If no files match, then throw an exception!
		if(files.size() == 0)
			throw new FileNotFoundException("There were no files that matched file pattern: '" + FILE_PATTERN + "'  within directory: " + rawDataDir);
		return files;
	}
    
    private void processGenes(String chrFile, String chr, String outfile) {
        String[] featureTypes = {
        		"gene" //CDS, mRNA, exon, ...
        };
        BioJavaRichSequence2JSON bj = new BioJavaRichSequence2JSON(chr, featureTypes); //just a placeholder...
        String[] paths = {
        		CoreAttributes._landmark.toString(),
        		CoreAttributes._minBP.toString(),
        		CoreAttributes._maxBP.toString()
        };
        // Unzip each file, parse it with Genbank tools to get a list of gene info,
        // Then "drain" this to individual items, drill out the paths above, 
        // then merge History back into a single line and write it to a file
        Pipe p = new Pipeline(new GenbankPipe(true), bj, new DrainPipe(), new SimpleDrillPipe(true, paths), new MergePipe("\t", true), new WritePipe(outfile));
        p.setStarts(Arrays.asList(new String[] {chrFile}));
        for(int i=0; p.hasNext(); i++){
            p.next();
        }
    }
}
