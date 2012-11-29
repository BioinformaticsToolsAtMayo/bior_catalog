package edu.mayo.bior.indexer.cmd;

import java.io.File;

import edu.mayo.bior.indexer.ColJsonPathPair;
import edu.mayo.bior.indexer.IndexUtils;
import edu.mayo.bior.indexer.SortExternal;

/** Create a text file with the keys and file positions where those keys are located, 
 *  then zip that file using bgzip
 * @author m054457
 */
public class BuildBgzipIndex {

	
	public static void main(String[] args) {
		try {
			if(args.length != 5) {
				usage();
				return;
			}
			
			IndexUtils utils = new IndexUtils();

			File bgzipFile = new File(args[0]);
			
			String delimiter = IndexUtils.getDelimFromCmdLine(args[1]);
			
			ColJsonPathPair colJsonPathPair = new ColJsonPathPair(args[2]);
			
			boolean isIntKey = "true".equalsIgnoreCase(args[3]);
			
			File txtIndexOutZip = new File(args[4]);
			File tmpTxt1 = new File(txtIndexOutZip.getParent() + "/tmpUnsorted.txt");
			File tmpTxt2 = new File(txtIndexOutZip.getParent() + "/tmpSorted.txt");
			
			utils.zipIndexesToTextFile(bgzipFile, delimiter, colJsonPathPair.column, colJsonPathPair.jsonPath, tmpTxt1);
			SortExternal.sortIndexFile(tmpTxt1, tmpTxt2, isIntKey);
			utils.bgzip(tmpTxt2, txtIndexOutZip);
			
			tmpTxt1.delete();
			tmpTxt2.delete();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void usage() {
		System.out.println("Build a sorted bgzip file which can be loaded into memory for in-memory search\n"
				+ "or used in the binary-file search");
		System.out.println("BuildBgzipIndex <bgzipDataFile> <delimiter> <keyColumn> <isKeyColumnInt> <bgzipIndexOut>");
		System.out.println("Ex:  BuildBgzipIndex  /data/variants.chr1.bgz  TAB  4:variant.rsId  false  /data/variants.chr1.index.rsid.bgz");
	}

}
