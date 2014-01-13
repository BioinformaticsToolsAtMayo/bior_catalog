package edu.mayo.bior.publishers.OMIM;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

import com.google.gson.JsonObject;

import edu.mayo.bior.indexer.IndexUtils;

public class LoadGenes {
    
	public static void usage(){
        System.out.println("usage: OmimPublisher <rawDataFile> <catalogOutputDir>");
    }
	
	public static void main(String[] args) {
		try {
			if(args.length < 2) {
	            usage();
	            System.exit(1);
			}
			new LoadGenes().exec(args[0], args[1]);
		}catch(Exception e) {
			e.printStackTrace();
		}
    }     
	
    /**
      Ex: 1.66|3|16|06|1p36.31|ESPN|P|Espin, mouse, homolog of||606351|R, H|||Deafness, autosomal recessive 36, 609006 (3); Deafness, neurosensory,|without vestibular involvement, autosomal dominant (3) | |4(espn)|
        1  - Numbering system, in the format  Chromosome.Map_Entry_Number
        2  - Month entered
        3  - Day 
        4  - Year
        5  - Cytogenetic location
        6  - Gene Symbol(s)
        7  - Gene Status (see below for codes)
        8  - Title
        9  - Title, cont. 1
        10 - MIM Number
        11 - Method (see below for codes)
        12 - Comments
        13 - Comments, cont. 1
        14 - Disorders (each disorder is followed by its MIM number, if
                different from that of the locus, and phenotype mapping method (see
                below).  Allelic disorders are separated by a semi-colon.
        15 - Disorders, cont. 1
        16 - Disorders, cont. 2
        17 - Mouse correlate
        18 - Reference
     * @throws IOException 
    */    
    public void exec(String fileInPath, String outputDir) throws IOException{
    	double start = System.currentTimeMillis();
        System.out.println("Started loading OMIM at: " + new Timestamp(new Date().getTime()));
        
        BufferedWriter fout = null;
        BufferedReader fin  = null;
        int numLines = 0;
        String line = null;
        try {
            File fileOut = new File(outputDir + "/scratch", "genemap_GRCh37.tsv");
            fileOut.getCanonicalFile().getParentFile().mkdirs();
            System.out.println("OMIM input file: " + fileInPath);

	        fout = new BufferedWriter(new FileWriter(fileOut));
	        fin = new BufferedReader(new FileReader(fileInPath));
	        while( (line = fin.readLine()) != null ) {
	        	numLines++;
	        	// Add ". 0 0" to beginning to signify chrom, minBP, maxBP to stay compatible with catalog design,
	        	// even though these fields are not available for genes
	        	fout.write(".\t0\t0\t" + toJson(line) + "\n");
	        }
	        fin.close();
	        fout.close();
	
	        System.out.println("Building bgzip file...");
	        File bgzipOut = new File(outputDir, fileOut.getName() + ".bgz");
	        new IndexUtils().bgzip(fileOut, bgzipOut);

	    	double end = System.currentTimeMillis();
	    	System.out.println("Lines processed: " + numLines);
	    	System.out.println("File size in:    " + new File(fileInPath).length());
	    	System.out.println("File size out:   " + fileOut.length());
	    	System.out.println("Bgzip file size: " + bgzipOut.length());
	    	System.out.println("Runtime: " + (end-start)/1000.0);
	    	System.out.println("DONE.");

        } catch(Exception e) {
        	e.printStackTrace();
        	System.out.println("Failed on line: " + numLines);
        	System.out.println("Line: " + line);
        } finally {
        	if( fin != null )
        		fin.close();
        	if( fout != null )
        		fout.close();        	
        }
        
    }

    private String toJson(String line) {
    	String[] cols = line.split("\\|", 18);
    	int i=0;
    	String chromMapEntryNum	= cols[i++];
    	String month 			= cols[i++];
    	String day				= cols[i++];
    	String year				= cols[i++];
    	String cytoLoc			= cols[i++];
    	String geneSymbols		= cols[i++];
    	String geneStatus		= cols[i++];
    	String title			= merge(cols[i++], cols[i++]);
    	String mimNum			= cols[i++];
    	String method			= cols[i++];
    	String comments			= merge(cols[i++], cols[i++]);
    	String disorders		= merge(cols[i++], cols[i++], cols[i++]);
    	String mouseCorrelate	= cols[i++];
    	String ref				= cols[i++];
    	
    	// NOTE: Only add those values that are non-null, non-empty-string
    	// NOTE: These columns should be numeric:
    	// 	Chromosome.Map_Entry_Number
    	//	MonthEntered
    	//	Day
    	//	Year
    	//	MIM_Number
    	JsonObject json = new JsonObject();
    	if(chromMapEntryNum.length() > 0)	json.addProperty("Chromosome.Map_Entry_Number", Double.parseDouble(chromMapEntryNum));
    	if(month.length() > 0)				json.addProperty("MonthEntered", 				Integer.parseInt(month));
    	if(day.length() > 0)				json.addProperty("Day", 						Integer.parseInt(day));
    	if(year.length() > 0)				json.addProperty("Year", 						Integer.parseInt(year));
    	if(cytoLoc.length() > 0)			json.addProperty("Cytogenetic_location", 		cytoLoc);
    	if(geneSymbols.length() > 0)		json.addProperty("GeneSymbols", 				geneSymbols);
    	if(geneStatus.length() > 0)			json.addProperty("Gene_Status", 				geneStatus);
    	if(title.length() > 0)				json.addProperty("Title", 						title);
    	if(mimNum.length() > 0)				json.addProperty("MIM_Number", 					Integer.parseInt(mimNum));
    	if(method.length() > 0)				json.addProperty("Method", 						method);
    	if(comments.length() > 0)			json.addProperty("Comments", 					comments);
    	if(disorders.length() > 0)			json.addProperty("Disorders", 					disorders);
    	if(mouseCorrelate.length() > 0)		json.addProperty("Mouse_correlate", 			mouseCorrelate);
    	if(ref.length() > 0)				json.addProperty("Reference", 					ref);
    	return json.toString();
    }
    
    private String merge(String... cols) {
    	StringBuilder str = new StringBuilder();
    	for(int i=0; i < cols.length; i++) {
    		str.append(cols[i] + " ");
    	}
    	return str.toString().trim();
    }
}
