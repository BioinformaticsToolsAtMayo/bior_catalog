package edu.mayo.bior.indexer.cmd;

import java.io.File;

import edu.mayo.bior.indexer.IndexUtils;
import edu.mayo.bior.indexer.SortExternal;

/** Create a Bgzip file from a text file
 *  by first sorting the text file (to a temporary file),
 *  then bgzip'ing the sorted text file
 * @author m054457
 */
public class BuildBgzip {

	public static void main(String[] args) {
		try {
			if(args.length != 3) {
				usage();
				return;
			}
			

			File txtFile = new File(args[0]);
			File bgzipFileOut = new File(args[1]);
			boolean isIntKey = "true".equalsIgnoreCase(args[2]);
			
			File txtTmpSorted = new File(bgzipFileOut.getParent() + "/tmpUnsorted.txt");
			
			IndexUtils utils = new IndexUtils();
			SortExternal.sortIndexFile(txtFile, txtTmpSorted, isIntKey);
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
