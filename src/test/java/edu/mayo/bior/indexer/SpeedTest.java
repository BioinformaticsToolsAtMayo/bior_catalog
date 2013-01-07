package edu.mayo.bior.indexer;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SpeedTest {

    /** this test does not work!!!
	// Catalog directory:
	private static String path = "/Users/m054457/Downloads/UcscDbSnp135/";
	// Data file
	private static File dataFileTxt = new File(path + "chr1.txt");
	private static File dataFileBgzip = new File(path + "chr1.txt.bgz");
	// Index file - text only
	private static File indexFileUnsorted = new File(path + "chr1.index.rsId.unsorted.txt");
	private static File indexFileSorted = new File(path + "chr1.index.rsId.sorted.txt");
	// Index file - zipped for reading into memory
	private static File indexFileGzip = new File(path + "chr1.index.rsId.sorted.txt.bgz");
	// Database index file
	private static File dbIndexFile = new File(path + "chr1.index.h2.db");
	// Random ids
	private static ArrayList<String> ids = new ArrayList<String>();
	
	@BeforeClass
	public static void initIndexes() throws Exception {
		IndexUtils utils = new IndexUtils();
		if(! dataFileTxt.exists())
			throw new Exception("Error!  Txt input file required!");
		if(! dataFileBgzip.exists())
			utils.bgzip(dataFileTxt, dataFileBgzip);
		if( ! indexFileSorted.exists() ) {
			utils.zipIndexesToTextFile(dataFileBgzip, "\t", 4, indexFileUnsorted);
			SortExternal.sortIndexFile(indexFileUnsorted, indexFileSorted, false);
		}
		if( ! indexFileGzip.exists() ) {
			utils.bgzip(indexFileSorted, indexFileGzip);
		}
		if( ! dbIndexFile.exists() ) {
			IndexerDatabase idxDb = new IndexerDatabase();
			idxDb.zipIndexesToDb(dataFileBgzip, 4, null, false, "\t", dbIndexFile);
		}
		if( ids.size() == 0 ) {
			System.out.println("Loading indexes from unsorted file (for ids to search on)...");
			List<String> lines = utils.loadLines(indexFileUnsorted);
			for(String line : lines) {
				ids.add(line.split("\t")[0]);
			}
			System.out.println("Num ids to search for: " + ids.size());
		}
	}
	
	
	@Test
	// Estimate: 14s for 4M lookups (286,000/s), 1.7GB mem used
	public void memory() throws IOException {
		System.out.println("=================================");
		System.out.println("Perform in-memory search...");
		double start = System.currentTimeMillis();
		IndexUtils utils = new IndexUtils();
		HashMap<String,List<Long>> fullIndex = utils.loadIndexBgzip(indexFileGzip);
		double end1 = System.currentTimeMillis();
		System.out.println("Time to load full zipped index into memory: " + (end1-start)/1000.0);
		IndexerMemory memIdx = new IndexerMemory();
		HashMap<String,List<Long>> idxMap = memIdx.findIndexes(ids, fullIndex);
		double end2 = System.currentTimeMillis();
		System.out.println("Time to find matches: " + (end2-end1)/1000.0);
		System.out.println("Runtime for bgzip load and in-memory search: " + (end2-start)/1000.0);
		long memUse = new IndexUtils().getMemoryUse() / (1024*1024);
		System.out.println("  Num matching keys: " + idxMap.size());
		System.out.println("  Mem use (MBs):  " + memUse);
	}
	
	@Test
	// Estimate: 79s for 4M lookups (50,600/s), 1GB mem used
	public void database() throws ClassNotFoundException, SQLException, IOException {
		System.out.println("=================================");
		System.out.println("Perform database search...");
		double start = System.currentTimeMillis();
		IndexerDatabase idxDb = new IndexerDatabase();
		Connection dbConn = idxDb.getConnectionH2(dbIndexFile);
		HashMap<String,List<Long>> idxMap = idxDb.findIndexes(ids, false, dbConn);
		double end = System.currentTimeMillis();
		System.out.println("Runtime for database search: " + (end-start)/1000.0);
		long memUse = new IndexUtils().getMemoryUse() / (1024*1024);
		System.out.println("  Num matching keys: " + idxMap.size());
		System.out.println("  Mem use (MBs):  " + memUse);
	}

	@Test
	// Estimate: 3200s for 4M lookups (1300/s), 900MB mem used
	public void file() throws IOException {
		System.out.println("=================================");
		System.out.println("Perform binary file search...");
		double start = System.currentTimeMillis();
		IndexerFile fileIdx = new IndexerFile();
		HashMap<String,List<Long>> idxMap = fileIdx.findIndexes(ids, indexFileSorted);
		double end = System.currentTimeMillis();
		System.out.println("Runtime for flat text file binary search: " + (end-start)/1000.0);
		long memUse = new IndexUtils().getMemoryUse() / (1024*1024);
		System.out.println("  Num matching keys: " + idxMap.size());
		System.out.println("  Mem use (MBs):  " + memUse);
	}
	
	@Test
	public void loadGeneNameIndex() throws IOException {
		System.out.println("=================================");
		System.out.println("Load gene index...");
		double start = System.currentTimeMillis();
		IndexUtils utils = new IndexUtils();
		File geneNameIndex = new File("genes.index.geneName.bgz");
		HashMap<String,List<Long>> fullIndex = utils.loadIndexBgzip(geneNameIndex);
		double end2 = System.currentTimeMillis();
		System.out.println("Time to load index: " + (end2-start)/1000.0);
		System.out.println("Runtime for bgzip load and in-memory search: " + (end2-start)/1000.0);
		long memUse = new IndexUtils().getMemoryUse() / (1024*1024);
		System.out.println("  Mem use (MBs):  " + memUse);
	}

* */
}
