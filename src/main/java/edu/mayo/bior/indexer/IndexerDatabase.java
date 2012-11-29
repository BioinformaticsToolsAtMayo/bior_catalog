package edu.mayo.bior.indexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class IndexerDatabase {

	private IndexUtils utils = new IndexUtils();
	
	public static void main(String[] args) {
		double start = System.currentTimeMillis();
		try {
			IndexerDatabase zip = new IndexerDatabase();
			zip.testVariants();
		}catch(Exception e) {
			e.printStackTrace();
		}
		double end = System.currentTimeMillis();
		System.out.println("DONE.  Runtime = " + (end - start)/1000.0);
	}

	public void testVariants() throws IOException, ClassNotFoundException, SQLException, NumberFormatException, InterruptedException {
		File bgzipFile = new File("resources/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz");
		File tmpTxt = new File("resources/tmp.txt");
		File queryResultTxt = new File("resources/queryResults.variants.fromDb.txt");
		File thousandGenomesDb = new File("resources/1000g.index");
		
		// First remove the database file
		File thousandGenomesDbFullFilePath = new File(thousandGenomesDb.getCanonicalPath() + ".h2.db");
		if(thousandGenomesDbFullFilePath.exists()) {
			System.out.println("Deleting file: " + thousandGenomesDbFullFilePath.getCanonicalPath());
			thousandGenomesDbFullFilePath.delete();
		}
		
	    Connection dbConn = getConnectionH2(thousandGenomesDb);
	    createTable(false, 200, dbConn);

		// Save ALL indexes, separated by tabs, based on rsId in column 3 (1-based)
	    System.out.println("Adding items to database...");
		//addZipIndexesToDb(bgzipFile, 3, false, "\t", dbConn);
	    double start = System.currentTimeMillis();
	    utils.zipIndexesToTextFile(bgzipFile, "\t", 3, null, tmpTxt);
	    double end = System.currentTimeMillis();
	    System.out.println("runtime: " + (end-start)/1000.0);
	    textIndexesToDb(dbConn, false, tmpTxt);
		System.out.println("Creating index on database...");
		createDbTableIndex(dbConn);

		// Given a set of sample keys / ids, get the rows from the bgzip file and save to txt file
		List<String> sampleKeys = utils.getSampleVariantIds();
		System.out.println("Find positions based on sample keys...");
		HashMap<String,List<Long>> key2posMap = findIndexes(sampleKeys, false, dbConn);
		System.out.println("Find lines in zip file based on positions...");
		HashMap<String,List<String>> key2LinesMap = utils.getZipLinesByIndex(bgzipFile, key2posMap);
		System.out.println("Write lines to file...");
		utils.writeLines(key2LinesMap, queryResultTxt);
        
        dbConn.close();
        System.out.println("DONE.");
	}
	
	
	public Connection getConnectionH2(File databaseFile) throws ClassNotFoundException, SQLException, IOException {
		Class.forName("org.h2.Driver");
		String dbPath = databaseFile.getCanonicalPath().replace(".h2.db", "");
		System.out.println("Database path: " + dbPath);
        String url = "jdbc:h2:file:" + dbPath + ";FILE_LOCK=SERIALIZED";
        double start = System.currentTimeMillis();
        Connection conn = DriverManager.getConnection(url, "sa", "");
        double end = System.currentTimeMillis();
        System.out.println("Time to connect to database: " + (end-start)/1000.0);
        return conn;
	}
	
	
	public Connection getConnectionHsqldb(File dbFile) throws ClassNotFoundException, SQLException, IOException {
		Class.forName("org.hsqldb.jdbcDriver");

        Connection conn =  DriverManager.getConnection(
        		"jdbc:hsqldb:" + dbFile.getCanonicalPath(),
        		"sa",
        		"");      
        return conn;
	}
	
	private void createTable(boolean isKeyInteger, int maxKeyLength, Connection dbConn) throws SQLException {
        final String SQL = "CREATE TABLE Indexer " 
        		+ "("
        		+   (isKeyInteger ? "Key BIGINT," : "Key VARCHAR(" + maxKeyLength + "), ")
        		+   "FilePos BIGINT" 
        		+ ")";
        Statement stmt = dbConn.createStatement();
        stmt.execute(SQL);
        stmt.close();
	}
	
	
	/** NOTE: For some reason, attempting to read from the bgzip file and write DIRECTLY to the database
	  * saves all of the strings into memory for the H2 pagestore, and those strings are ***NOT*** garbage collected
	  * WHICH BLOWS THE HEAP!!!!!!!!!!!!!!!!!!!!!!!!
	  * INSTEAD, this will first extract the key,fileposition pairs to a temporary text file, THEN write to the database.
	  */
	public void zipIndexesToDb(File bgzipFile, int keyColumn, String jsonPathToKey, boolean isKeyInteger, String delimiter, File newDb)
	 throws NumberFormatException, SQLException, IOException, InterruptedException, ClassNotFoundException {
		// Write the indexes to a flat text file
		File tmpTxtIdxFile = new File(bgzipFile.getParent() + "/tmpIdx.txt");
		utils.zipIndexesToTextFile(bgzipFile, delimiter, keyColumn, jsonPathToKey, tmpTxtIdxFile);
		
		int maxKeyLength = getMaxKeyLength(tmpTxtIdxFile);
		
		// Read the text file and write to database
		BufferedReader fin = new BufferedReader(new FileReader(tmpTxtIdxFile)); //"/Users/m054457/Downloads/UcscDbSnp135/chr1.index.rsId.sorted.txt")); //tmpTxtIdxFile));
		
		Connection dbConn = getConnectionH2(newDb);
		createTable(isKeyInteger, maxKeyLength, dbConn);
		
		final String SQL = "INSERT INTO Indexer (Key, FilePos) VALUES (?, ?)";
		PreparedStatement stmt = dbConn.prepareStatement(SQL);
		dbConn.setAutoCommit(true);
		String line = null;
		while( (line = fin.readLine()) != null ) {
			String key = utils.getCol(line, delimiter, 1);
			Long pos = Long.valueOf(utils.getCol(line, delimiter, 2));
			if(isKeyInteger)
				stmt.setLong(1, Integer.valueOf(key));
			else
				stmt.setString(1, key);
			stmt.setLong(2, pos);
			stmt.execute();
		}

		//tmpTxtIdxFile.delete();
		createDbTableIndex(dbConn);
		stmt.close();
		dbConn.close();
		fin.close();
	}
	
	
	private int getMaxKeyLength(File tmpTxtIdxFile) throws IOException {
		BufferedReader fin = new BufferedReader(new FileReader(tmpTxtIdxFile));
		String line = null;
		int maxKeyLen = 0;
		while( (line = fin.readLine()) != null ) {
			String key = line.split("\t")[0];
			if(key.length() > maxKeyLen)
				maxKeyLen = key.length();
		}
		return maxKeyLen;
	}

	private void textIndexesToDb(Connection dbConn, boolean isKeyInteger, File tmpTxt) throws NumberFormatException, SQLException, IOException {
		long numObjects = 0;
		long numBytesRead = 0;
		long MB = 1024L * 1024L;

		BufferedReader fin = new BufferedReader(new FileReader(tmpTxt));
		
		final String SQL = "INSERT INTO Indexer (Key, FilePos) VALUES (?, ?)";
		PreparedStatement stmt = dbConn.prepareStatement(SQL);
		dbConn.setAutoCommit(true);

		String line = null;
		while( (line = fin.readLine()) != null ) {
			numObjects++;
			String[] cols = line.split("\t");
			String key = cols[0];
			String pos = cols[1];
			if(isKeyInteger)
				stmt.setLong(1, Integer.valueOf(key));
			else
				stmt.setString(1, key);
			stmt.setLong(2, Long.valueOf(pos));
			stmt.execute();
			dbConn.commit();

			if( numObjects % 10000 == 0 ) {
				System.out.println(key + "    " + numObjects 
						+ ", avgBytesPerItem: " + (numBytesRead / numObjects) 
						+ ", MBs read: " + (numBytesRead/MB) + ", Mem (MBs): " + (utils.getMemoryUse()/MB));
			}
		} while( line != null );

		fin.close();
		stmt.close();
		
		System.out.println("Num objects read: " + numObjects);
	}
	
	
	private void createDbTableIndex(Connection dbConn) throws SQLException {
		 final String SQL = "CREATE INDEX keyIndex ON Indexer (Key);";
		 Statement stmt = dbConn.createStatement();
		 stmt.execute(SQL);
		 stmt.close();
	}
	
	/** Find file positions within zip file matching ids 
	 * @throws SQLException */
	public HashMap<String,List<Long>> findIndexes(List<String> idsToFind, boolean isKeyInteger, Connection dbConn) throws SQLException {
		final String SQL = "SELECT FilePos FROM Indexer WHERE Key = ?";
		PreparedStatement stmt = dbConn.prepareStatement(SQL);
		HashMap<String,List<Long>> key2posMap = new HashMap<String,List<Long>>();
		int count = 0;
		// Remove any duplicate ids by assigning to a HashSet
		double start = System.currentTimeMillis();
		Set<String> idSet = new HashSet<String>(idsToFind);
		Iterator<String> it = idSet.iterator();
		double end = System.currentTimeMillis();
		System.out.println("Time to create set from list: " + (end-start)/1000.0);
		System.out.println("Set size: " + idSet.size());
		IndexUtils utils = new IndexUtils();
		long maxMem = 0;
		while(it.hasNext()) {
			String id = it.next();
			count++;
			if(count % 100000 == 0 ) {
				//System.out.println(".");
				double now = System.currentTimeMillis();
				int numPerSec = (int)(count/((now-end)/1000.0));
				long mem = utils.getMemoryUseMB();
				if(mem > maxMem)
					maxMem = mem;
				System.out.println(count + "\t #/sec: " + numPerSec + "\t Est time: " + ((idSet.size()-count)/numPerSec) + " s  " + mem + "MB");
			}
			List<Long> positions = key2posMap.get(id);
			if(positions == null) {
				positions = new ArrayList<Long>();
				key2posMap.put(id, positions);
			}
			
			if(isKeyInteger)
				stmt.setLong(1, Long.valueOf(id));
			else
				stmt.setString(1, id);
			
			ResultSet rs = stmt.executeQuery();
			while(rs.next()) {
				Long pos = rs.getLong("FilePos");
				positions.add(pos);
			}
			rs.close();
		}
		System.out.println("Max memory: " + maxMem);
		stmt.close();
		return key2posMap;
	}
	

}
