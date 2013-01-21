package edu.mayo.bior.indexer.cmd;

import java.io.File;

import edu.mayo.bior.indexer.IndexUtils;
import edu.mayo.bior.indexer.SortExternal;

/** Sort a text file
 * @author m054457
 */
public class SortFile {
	
	public static void main(String[] args) {
		try {
			if(args.length != 3) {
				usage();
				return;
			}

			File txtFileIn = new File(args[0]);
			File bgzipFileOut = new File(args[1]);
			boolean isIntKey = "true".equalsIgnoreCase(args[2]);
			
			String parentFolder = new File(bgzipFileOut.getCanonicalPath()).getParent();
			File txtTmpSorted = new File(parentFolder + "/tmpUnsorted.txt");
			
			IndexUtils utils = new IndexUtils();
			SortExternal.sortIndexFile(txtFileIn, txtTmpSorted, isIntKey);
			utils.bgzip(txtTmpSorted, bgzipFileOut);
			
			txtTmpSorted.delete();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void usage() {
		System.out.println("Build a sorted bgzip file from a text file (with tab-separated columns)");
		System.out.println("BuildBgzip <txtFile> <bgzipFileOut> <isFirstColumnAnInteger>");
		System.out.println("Ex:  BuildBgzip  /data/variants.chr1.txt  /data/variants.chr1.bgz false");
	}

}
