package edu.mayo.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import com.jayway.jsonpath.JsonPath;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.transform.TransformFunctionPipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.UNIX.CatGZPipe;
import edu.mayo.pipes.UNIX.GrepPipe;
import edu.mayo.pipes.bioinformatics.sequence.Bed2SequencePipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;

/** Validate a newly-built catalog by comparing all of its refAlleles at their positions
 *  with known refAlleles from the NCBIGenomes data files.
 *  Takes a bunch of variants from a newly-built catalog and compares those variants
 *  against a target fasta file (that has been tabix-indexed) that we know is one-based
 *  to verify these all match:
 *    - chromosome
 *    - minBP
 *    - refAllele
 *  and that the refAllele is not null or blank or '.'
 *  Have it match on at least 20 variants to have a high level of confidence that the catalog is 1-based.
 *  (checking 20 variants equates to a 1 in 1.1 trillion chance that all variants pass the check but that the 
 *  catalog is actually still 0-based:  0.25^20 = 9.09E13 
 *  which assumes 0.25 chance base-pair will match another base-pair))
 * ---------
 * @author Michael Meiners
 *
 */
public class OneBasedCatalogValidator {
	
	public int mTotalLines = 0;
	public int mMismatches = 0;
	public int mNotFound	= 0;
	public int mUnknownRef = 0;
	
	private ArrayList<Integer> mBadRows = new ArrayList<Integer>();

	
	public static void usage() {
		System.out.println("Validate that all variants within the given catalog file match the reference allele from the trusted NCBIGenome build GRCh37 fasta file");
		System.out.println("All variants will be checked unless you specify a particular chromosome.");
		System.out.println("OneBasedCatalogValidator  <catalogFilePath>  <fastaWithAllRefsFilePath>  [chromosomeToRestrictSearchTo]");
		System.out.println("NOTE: <fastaWithAllRefs> must refer to the NCBIGenome GRCh37 bgzip fasta file ");
		System.out.println("        which has an associated tabix index with the extension .tbi in the same directory)");
		System.out.println("NOTE: [chomosomeToRestrictSearchTo] is optional");
		System.out.println("Ex: OneBasedCatalogValidator ");
		System.out.println("        /data/catalogs/BGI/hg19/LuCAMP_200exomeFinal_hg19.tsv.bgz");
		System.out.println("        /data/catalogs/NCBIGenome/GRCh37.p10/hs_ref_genome.fa.tsv.bgz");
		System.out.println("        22");
	}
	
	
	
	public static void main(String[] args) {
		if( args.length != 2 && args.length != 3) {
			usage();
			System.exit(1);
		}
		
		try {
			String chrom = args.length > 2  ?  args[2] : null;
			int numBadRows = new OneBasedCatalogValidator().verifyOneBased(args[0], args[1], chrom);
			if(numBadRows > 0)
				System.out.println("Catalog checks out - it is in fact one-based");
			else
				System.out.println("ERROR!  Catalog is either not one-based or needs liftOver performed on the chromosome positions.");
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/** Checks all reference alleles within the catalog and compares those against the known
	 *  reference alleles at the same one-based position within the fasta file (known to be good one-based ref alleles)
	 * @param catalogPath  Path to catalog file that is to be tested
	 * @param fastaPath  Path to fasta file that we will validate position and ref allele against
	 * @param chrInFastaFile  Chromosome to confine the search to (so we can have smaller fasta files for testing).
	 *                        This should match "1","2".."22","M","X","Y".
	 * @return Number of bad matches
	 * @throws IOException 
	 * @throws Exception 
	 */
	public int verifyOneBased(String catalogPath, String fastaPath, String chrInFastaFile) throws IOException {
		System.out.println("Checking the refAlleles against a known refAllele one-based source...");
		String grepRegEx = ".*";
		if(chrInFastaFile != null) 
			grepRegEx = "^" + chrInFastaFile + "\t";
		
		if( ! new File(catalogPath).exists() )
			throw new FileNotFoundException("catalog file does not exist: " + catalogPath);
		if( ! new File(fastaPath).exists() )
			throw new FileNotFoundException("Fasta reference file does not exist: " + fastaPath);
		
		Pipeline pipe = new Pipeline(
				// Load new catalog
				new CatGZPipe("gzip"),
				// Restrict to chromosome if user specifies it (for grep - specify that chrom is at beginning of line followed by a tab)
				new GrepPipe(grepRegEx),
				// Convert to history
				new HistoryInPipe(),
				new PrintPipe(),
				// Search fasta file by position
				//new TabixSearchHistoryPipe(fastaPath),
				new Bed2SequencePipe(fastaPath, true),
				// Compare chromosome, position, refAllele
				new TransformFunctionPipe<History,History>(new SameAlleleFunction())
				//new PrintPipe()
		);
		
		// NOTE: Should be able to use Dan's BedToFasta class
		
		pipe.setStarts(Arrays.asList(catalogPath));
		
		// Show a dot for every 1000 lines processed
		System.out.println("(.=1000 processed,  o=10k,  O=100k)");
		while(pipe.hasNext())
			pipe.next();
		// Print a return after all the '.'s
		System.out.println();
	
		printResults();
		return  mBadRows.size();
	}
	
	private void printResults() {
		System.out.println("Num total lines: " + mTotalLines);
		System.out.println("Num mismatches:  " + mMismatches);
		System.out.println("Num not found (tabix found nothing at that position): " + mNotFound);
		System.out.println("Num unknown (NCBIGenome had an 'N' in that position): " + mUnknownRef);

		System.out.println("Bad rows (1-based): ");
		for(int row : mBadRows) {
			System.out.print(row + ", ");
		}
		System.out.println();
	}
	
    public class SameAlleleFunction implements PipeFunction<History,History>{
        private JsonPath refAllelePath;

        public SameAlleleFunction() {
            refAllelePath 	= JsonPath.compile(CoreAttributes._refAllele.toString());     
        }
        
		@Override
		public History compute(History history) {
			mTotalLines++;
			
			printProgress();

			// No known ref found (from tabix search) for this line
			if( history.size() < 5 )  {
				mNotFound++;
				mBadRows.add(mTotalLines);
				return history;
			}

			// Get info from variants to search on
			String json = history.get(history.size() - 2);
			String ref  = refAllelePath.read(json);

			String refAlleleKnown = history.get(history.size() - 1);
			
			if( refAlleleKnown.equalsIgnoreCase("N") ) {
				mUnknownRef++;
				mBadRows.add(mTotalLines);
			} else if( ! ref.equalsIgnoreCase(refAlleleKnown) ) {
				mMismatches++;
				mBadRows.add(mTotalLines);
			}
			
			return history;
		}

		private void printProgress() {
			if(mTotalLines > 0 && (mTotalLines % 100000) == 0)
				System.out.print("O");
			else if(mTotalLines > 0 && (mTotalLines % 10000) == 0)
				System.out.print("o");
			else if(mTotalLines > 0 && (mTotalLines % 1000 == 0) )
				System.out.print(".");
		}
    }

}
