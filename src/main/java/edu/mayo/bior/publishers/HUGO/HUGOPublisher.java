/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.HUGO;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.bior.publishers.OMIM.LoadGenes;
import edu.mayo.pipes.HeaderPipe;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrependStringPipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.WritePipe;
import edu.mayo.pipes.JSON.InjectIntoJsonPipe;
import edu.mayo.pipes.JSON.inject.ColumnArrayInjector;
import edu.mayo.pipes.JSON.inject.ColumnInjector;
import edu.mayo.pipes.JSON.inject.Injector;
import edu.mayo.pipes.JSON.inject.JsonType;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.history.HCutPipe;
import edu.mayo.pipes.history.HistoryInPipe;

/**
 *
 * @author m102417
 */
public class HUGOPublisher {
	
    public static void main(String[] args) throws Exception {
    	
    	/*
        //String hgncfile = "/data/hgnc/2013_02_12/hgnc_complete_dataset.txt";        
    	String hgncfile = "c:\\mayo\\bior\\hgnc_hugo\\hgnc_complete_dataset.txt";
        System.out.println("Parsing: " + hgncfile);
        HUGOPublisher hgnc = new HUGOPublisher();
        hgnc.exec(hgncfile, new PrintPipe());
        */
    	
    	HUGOPublisher publisher = new HUGOPublisher();      
        
        if(args.length >= 1){ 
            publisher.exec(args[0], args[1] + "/scratch/");
        }else{
            usage();
            System.exit(1);
        }
    }
    
    public static void usage(){
        System.out.println("usage: HGNCPublisher <rawDataFile> <catalogOutputDir>");
    }
    
	/** FORMAT
	0. HGNC ID	
	1. Approved Symbol
	2. Approved Name
	3. Status
	4. Locus Type
	5. Locus Group
	6. Previous Symbols
	7. Previous Names
	8. Synonyms
	9. Name Synonyms
	10. Chromosome
	11. Date Approved
	12. Date Modified
	13. Date Symbol Changed
	14. Date Name Changed
	15. Accession Numbers
	16. Enzyme IDs
	17. Entrez Gene ID
	18. Ensembl Gene ID
	19. Mouse Genome Database ID
	20. Specialist Database Links
	21. Specialist Database IDs
	22. Pubmed IDs
	23. RefSeq IDs
	24. Gene Family Tag
	25. Gene family description
	26. Record Type	
	27. Primary IDs
	28. Secondary IDs
	29. CCDS IDs
	30. VEGA IDs
	31. Locus Specific Databases
	32. GDB ID (mapped data)
	33. Entrez Gene ID (mapped data supplied by NCBI)
	34. OMIM ID (mapped data supplied by NCBI)
	35. RefSeq (mapped data supplied by NCBI)
	36. UniProt ID (mapped data supplied by UniProt)
	37. Ensembl ID (mapped data supplied by Ensembl)
	38. UCSC ID (mapped data supplied by UCSC)
	39. Mouse Genome Database ID (mapped data supplied by MGI)
	40. Rat Genome Database ID (mapped data supplied by RGD)
	*/    
    public void exec(String file, String outputDir) {
    	
    	final String catalogFile = "hgnc_GRCh37.tsv";
    	
        System.out.println("Started loading HGNC/HUGO at: " + new Timestamp(new Date().getTime()));
        
        //String outfile = outputDir + "\\" + catalogFile; 
        String outfile = outputDir + catalogFile;        
        System.out.println("Outputing File to: " + outfile);

        WritePipe writePipe = new WritePipe(outfile);
    	
        int[] ccols = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41}; 
        int c = 1;
        InjectIntoJsonPipe inject = new InjectIntoJsonPipe(true, new Injector[] {
        	// Columns are 1-based (so adding the 1)
            new ColumnInjector(c++, "HGNC_ID", JsonType.STRING),
            new ColumnInjector(c++, "Approved_Symbol", JsonType.STRING),
            new ColumnInjector(c++, "Approved_Name", JsonType.STRING),
            new ColumnInjector(c++, "Status", JsonType.STRING),
            new ColumnInjector(c++, "Locus_Type", JsonType.STRING),
            new ColumnInjector(c++, "Locus_Group", JsonType.STRING),
            new ColumnArrayInjector(c++,"Previous_Symbols", JsonType.STRING, ",", true),
            new ColumnArrayInjector(c++,"Previous_Names", JsonType.STRING, ",", true),
            new ColumnArrayInjector(c++,"Synonyms", JsonType.STRING, ",", true),
            new ColumnArrayInjector(c++,"Name_Synonyms", JsonType.STRING, ",", true),
            new ColumnInjector(c++, "Chromosome", JsonType.STRING),
            new ColumnInjector(c++, "Date_Approved", JsonType.STRING),
            new ColumnInjector(c++, "Date_Modified", JsonType.STRING),
            new ColumnInjector(c++, "Date_Symbol_Changed", JsonType.STRING),
            new ColumnInjector(c++, "Date_Name_Changed", JsonType.STRING),
            new ColumnArrayInjector(c++,"Accession_Numbers", JsonType.STRING, ",", true),
            new ColumnArrayInjector(c++,"Enzyme_IDs", JsonType.STRING, ",", true),
            new ColumnInjector(c++, "Entrez_Gene_ID", JsonType.STRING),
            new ColumnInjector(c++, "Ensembl_Gene_ID", JsonType.STRING),
            new ColumnInjector(c++, "Mouse_Genome_Database_ID", JsonType.STRING),
            new ColumnInjector(c++, "Specialist_Database_Links", JsonType.STRING),
            new ColumnArrayInjector(c++,"Specialist_Database_IDs", JsonType.STRING, ",", true),
            new ColumnArrayInjector(c++,"Pubmed_IDs", JsonType.STRING, ",", true),
            new ColumnArrayInjector(c++,"RefSeq_IDs", JsonType.STRING, ",", true),
            new ColumnInjector(c++, "Gene_Family_Tag", JsonType.STRING),
            new ColumnInjector(c++, "Gene_family_description", JsonType.STRING),
            new ColumnInjector(c++, "Record_Type", JsonType.STRING),
            new ColumnArrayInjector(c++,"Primary_IDs", JsonType.STRING, ",", true),
            new ColumnArrayInjector(c++,"Secondary_IDs", JsonType.STRING, ",", true),
            new ColumnArrayInjector(c++,"CCDS_IDs", JsonType.STRING, ",", true),
            new ColumnArrayInjector(c++,"VEGA_IDs", JsonType.STRING, ",", true),
            new ColumnInjector(c++, "Locus_Specific_Databases", JsonType.STRING),
            new ColumnInjector(c++, "mapped_GDB_ID", JsonType.STRING),
            new ColumnInjector(c++, "mapped_Entrez_Gene_ID", JsonType.STRING),
            new ColumnInjector(c++, "mapped_OMIM_ID", JsonType.STRING),
            new ColumnInjector(c++, "mapped_RefSeq", JsonType.STRING),
            new ColumnInjector(c++, "UniProt_ID", JsonType.STRING),
            new ColumnInjector(c++, "mapped_Ensembl_ID", JsonType.STRING),
            new ColumnInjector(c++, "UCSC_ID", JsonType.STRING),
            new ColumnInjector(c++, "mapped_Mouse_Genome_Database_ID", JsonType.STRING),
            new ColumnInjector(c++, "mapped_Rat_Genome_Database_ID", JsonType.STRING)
        });

        Pipe p = new Pipeline(
                new CatPipe(), 
                new HeaderPipe(1), 
                new HistoryInPipe(),
                inject,
                new HCutPipe(ccols),
                new MergePipe("\t", true),
                new PrependStringPipe(".\t0\t0\t"),
                writePipe
                );
        p.setStarts(Arrays.asList(file));
        for(int i=0; p.hasNext(); i++){
            p.next();
            //if(i==5) break;
        }
    }

    
}


