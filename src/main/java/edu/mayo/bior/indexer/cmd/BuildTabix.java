package edu.mayo.bior.indexer.cmd;

import java.io.File;

import edu.mayo.bior.indexer.IndexUtils;
import edu.mayo.bior.indexer.SortExternal;


// TODO: Modify this to build a tabix file


/** Create a Tabix index file from a bgzip'd file (of a sorted tab-delimited text file) 
 *  whose first three columns are chromosome, minBP, maxBP
 * @author m054457
 */
public class BuildTabix {

	public static void main(String[] args) {
		try {
			if(args.length != 2) {
				usage();
				return;
			}
			

//			File txtFile = new File(args[0]);
//			File bgzipFileOut = new File(args[1]);
//			boolean isIntKey = "true".equalsIgnoreCase(args[2]);
//			
//			File txtTmpSorted = new File(bgzipFileOut.getParent() + "/tmpUnsorted.txt");
//			
//			IndexUtils utils = new IndexUtils();
//			SortExternal.sortIndexFile(txtFile, txtTmpSorted, isIntKey);
//			utils.bgzip(txtTmpSorted, bgzipFileOut);
//			
//			txtTmpSorted.delete();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void usage() {
		System.out.println("Build a tabix index file from a bgzip'd file (of a sorted tab-delimited text file)");
		System.out.println("whose first three columns are chromosome, minBP, maxBP");
		System.out.println("BuildTabix <bgzipFile> <tabixIndexFileOut>");
		System.out.println("Ex:  BuildTabix  /data/variants.chr1.bgz  /data/variants.chr1.tbi");
	}

}
