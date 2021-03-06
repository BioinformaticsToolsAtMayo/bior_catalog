package edu.mayo.bior.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import com.jayway.jsonpath.JsonPath;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.transform.TransformFunctionPipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.MergePipe;
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
	
	public static void usage() {
		System.out.println("Validate that all variants within the given catalog file match the reference allele from the trusted NCBIGenome build GRCh37 fasta file");
		System.out.println("All variants will be checked unless you specify a particular chromosome.");
		System.out.println("OneBasedCatalogValidator  <catalogFilePath>  <fastaWithAllRefsFilePath>  <isPrintBadRows>  [chromosomeToRestrictSearchTo]");
		System.out.println("NOTE: <fastaWithAllRefs> must refer to the NCBIGenome GRCh37 bgzip fasta file ");
		System.out.println("        which has an associated tabix index with the extension .tbi in the same directory)");
		System.out.println("NOTE: [chomosomeToRestrictSearchTo] is optional");
		System.out.println("Ex: OneBasedCatalogValidator ");
		System.out.println("        /data/catalogs/BGI/hg19/LuCAMP_200exomeFinal_hg19.tsv.bgz");
		System.out.println("        /data/catalogs/NCBIGenome/GRCh37.p10/hs_ref_genome.fa.tsv.bgz");
		System.out.println("        true");
		System.out.println("        22");
	}
	
	
	
	public static void main(String[] args) {
		if( args.length != 3 && args.length != 4) {
			usage();
			System.exit(1);
		}
		
		try {
			boolean isPrintRows = Boolean.parseBoolean(args[2]);
			String chrom = args.length > 3  ?  args[3] : null;
			new OneBasedCatalogValidator().verifyOneBased(args[0], args[1], isPrintRows, chrom);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public int verifyOneBased(String catalogPath, String fastaPath) throws Exception {
		return this.verifyOneBased(catalogPath, fastaPath, false, null);
	}
	
	/** Checks all reference alleles within the catalog and compares those against the known
	 *  reference alleles at the same one-based position within the fasta file (known to be good one-based ref alleles)
	 * @param catalogPath  Path to catalog file that is to be tested
	 * @param fastaPath  Path to fasta file that we will validate position and ref allele against
	 * @param chrInFastaFile  Chromosome to confine the search to (so we can have smaller fasta files for testing).
	 *                        This should match "1","2".."22","M","X","Y".
	 * @return Number of bad matches
	 * @throws Exception 
	 */
	public int verifyOneBased(String catalogPath, String fastaPath, boolean isPrintRows, String chrInFastaFile) throws Exception {
		System.out.println("Checking the refAlleles against a known refAllele one-based source...");
		String grepRegEx = ".*";
		if(chrInFastaFile != null) 
			grepRegEx = "^" + chrInFastaFile + "\t";
		
		if( ! new File(catalogPath).exists() )
			throw new FileNotFoundException("catalog file does not exist: " + catalogPath);
		if( ! new File(fastaPath).exists() )
			throw new FileNotFoundException("Fasta reference file does not exist: " + fastaPath);
		
		LastLineFunction lastLineFunc = new LastLineFunction();
		
		Pipeline pipe = new Pipeline(
				// Load new catalog
				new CatGZPipe("gzip"),
				// Restrict to chromosome if user specifies it (for grep - specify that chrom is at beginning of line followed by a tab)
				new GrepPipe(grepRegEx),
				// Convert to history
				new HistoryInPipe(),
				new TransformFunctionPipe<History,History>(lastLineFunc),
				//new PrintPipe(),
				// Search fasta file by position
				//new TabixSearchHistoryPipe(fastaPath),
				new Bed2SequencePipe(fastaPath, true),
				// Compare chromosome, position, refAllele
				new TransformFunctionPipe<History,History>(new SameAlleleFunction(isPrintRows)),
				new MergePipe("\t")
				//new PrintPipe()
		);
		
		// NOTE: Should be able to use Dan's BedToFasta class
		
		pipe.setStarts(Arrays.asList(catalogPath));
		
		// Show a dot for every 1000 lines processed
		System.out.println("(.=1K processed,  o=10K,  O=100K)");
		try {
			while(pipe.hasNext())
				pipe.next();
		}catch(Exception e) {
			System.err.println("\nLine we choked on:");
			System.err.println("-----------------------------------");
			System.err.println(lastLineFunc.mLastLine);
			System.err.println("-----------------------------------");
			e.printStackTrace();
			throw e;
		}
		// Print a return after all the '.'s so next statement is on a new line
		System.out.println();
	
		printResults();
		return  (mMismatches + mNotFound + mUnknownRef);
	}
	
	private void printResults() {
		System.out.println("Num total lines: " + mTotalLines);
		System.out.println("Num mismatches:  " + mMismatches);
		System.out.println("Num not found (tabix found nothing at that position, or chromosome not found (ex: chromosome 'M')): " + mNotFound);
		System.out.println("Num unknown (NCBIGenome had an 'N' in that position): " + mUnknownRef);
		
		int numBadRows = mMismatches + mNotFound + mUnknownRef;
		double percentMismatch = 100 * ((double)mMismatches)/((double)mTotalLines);
		if(numBadRows == 0)
			System.out.println("Catalog checks out - it is in fact one-based, and there were no mismatches, not-founds, or unknown ref alleles.");
		else if(percentMismatch < 1)
			System.out.println("WARNING: There are some ref allele mismatches, but they are under 1% (" + percentMismatch + ").");
		else
			System.out.println("ERROR!  Catalog is either not one-based or needs liftOver performed on the chromosome positions. Percent mismatches: " + percentMismatch);
	}

    public class LastLineFunction implements PipeFunction<History,History>{
    	public String mLastLine = null;
    	
    	@Override
		public History compute(History history) {
    		mLastLine = history.getMergedData("\t");
    		return history;
		}
    }

    public class SameAlleleFunction implements PipeFunction<History,History>{
        private JsonPath mRefAllelePath;
        private boolean mIsPrintRows = false;

        public SameAlleleFunction(boolean isPrintRows) {
        	mIsPrintRows = isPrintRows;
        	mRefAllelePath 	= JsonPath.compile(CoreAttributes._refAllele.toString());
        }
        
		@Override
		public History compute(History history) {
			mTotalLines++;
			
			printProgress();

			// No known ref found (from tabix search) for this line
			if( history.size() < 5 || history.get(4).equals("."))  {
				mNotFound++;
				printRow("=============== Known Ref not found at the specified position:", history);
				return history;
			}

			// Get info from variants to search on
			String json = history.get(history.size() - 2);
			String ref  = mRefAllelePath.read(json);

			String refAlleleKnown = history.get(history.size() - 1);
			
			if( refAlleleKnown.equalsIgnoreCase("N") ) {
				mUnknownRef++;
				printRow("=============== Unknown reference (Ref: " + ref + ", NCBI: N): Line " + mTotalLines, history);
			} else if( ! ref.equalsIgnoreCase(refAlleleKnown) ) {
				mMismatches++;
				printRow("=============== MISMATCH (Ref: " + ref + ", NCBI: " + refAlleleKnown + "): Line " + mTotalLines, history);
			}
			
			return history;
		}
		
		private void printRow(String msg, History row) {
			if(mIsPrintRows) {
				System.out.println("\n" + msg);
				System.out.println(row.getMergedData("\t"));
			}
		}

		private void printProgress() {
			if(mTotalLines > 0 && (mTotalLines % 100000) == 0)
				System.out.println("O");
			else if(mTotalLines > 0 && (mTotalLines % 10000) == 0)
				System.out.print("o");
			else if(mTotalLines > 0 && (mTotalLines % 1000 == 0) )
				System.out.print(".");
		}
    }

}
