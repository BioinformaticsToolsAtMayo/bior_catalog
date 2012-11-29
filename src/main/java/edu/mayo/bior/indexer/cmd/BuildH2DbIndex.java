package edu.mayo.bior.indexer.cmd;

import java.io.File;

import edu.mayo.bior.indexer.ColJsonPathPair;
import edu.mayo.bior.indexer.IndexUtils;
import edu.mayo.bior.indexer.IndexerDatabase;

/** Create a text file with the keys and file positions where those keys are located,
 *  find the largest key size (to determine the varchar size in the database)
 *  then add all of those keys and file positions to the H2 database
 * @author m054457
 */
public class BuildH2DbIndex {

	
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
			
			File dbFile = new File(args[4]);
			File tmpTxt1 = new File(dbFile.getParent() + "/tmpUnsorted.txt");
			
			IndexerDatabase dbIdx = new IndexerDatabase();
			dbIdx.zipIndexesToDb(bgzipFile, colJsonPathPair.column, colJsonPathPair.jsonPath, isIntKey, delimiter, dbFile);
			tmpTxt1.delete();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void usage() {
		System.out.println("Build an H2 file-based database containing key,filePosition columns\n"
				+ "which can be used in a database search");
		System.out.println("BuildH2DbIndex <bgzipDataFile> <delimiterInBgzip> <keyColumn> <isKeyColumnInt> <h2DatabaseName>");
		System.out.println("Ex:  BuildH2DbIndex  /data/variants.chr1.bgz  TAB  4:variant.rsId  false  /data/variants.chr1.index.rsid.h2.db");
	}

}
