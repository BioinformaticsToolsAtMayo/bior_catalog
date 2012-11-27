package edu.mayo.bior.indexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import net.sf.samtools.util.BlockCompressedInputStream;

public class IndexerDatabase extends IndexUtils {

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
	    createTable(false, dbConn);

		// Save ALL indexes, separated by tabs, based on rsId in column 3 (1-based)
	    System.out.println("Adding items to database...");
		//addZipIndexesToDb(bgzipFile, 3, false, "\t", dbConn);
	    double start = System.currentTimeMillis();
	    zipIndexesToTextFile(bgzipFile, "\t", 3, null, tmpTxt);
	    double end = System.currentTimeMillis();
	    System.out.println("runtime: " + (end-start)/1000.0);
	    textIndexesToDb(dbConn, false, tmpTxt);
		System.out.println("Creating index on database...");
		createDbTableIndex(dbConn);

		// Given a set of sample keys / ids, get the rows from the bgzip file and save to txt file
		List<String> sampleKeys = getSampleVariantIds();
		System.out.println("Find positions based on sample keys...");
		HashMap<String,List<Long>> key2posMap = findIndexes(sampleKeys, false, dbConn);
		System.out.println("Find lines in zip file based on positions...");
		HashMap<String,List<String>> key2LinesMap = getZipLinesByIndex(bgzipFile, key2posMap);
		System.out.println("Write lines to file...");
		writeLines(key2LinesMap, queryResultTxt);
        
        dbConn.close();
        System.out.println("DONE.");
	}
	
	
	public Connection getConnectionH2(File databaseFile) throws ClassNotFoundException, SQLException, IOException {
		Class.forName("org.h2.Driver");
		System.out.println("Database path: " + databaseFile.getCanonicalPath());
        String url = "jdbc:h2:file:" + databaseFile.getCanonicalPath();
        Connection conn = DriverManager.getConnection(url, "sa", "");
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
	
	public void createTable(boolean isKeyInteger, Connection dbConn) throws SQLException {
        final String SQL = "CREATE TABLE Indexer " 
        		+ "("
        		+   (isKeyInteger ? "Key BIGINT," : "Key VARCHAR(200), ")
        		+   "FilePos BIGINT" 
        		+ ")";
        Statement stmt = dbConn.createStatement();
        stmt.execute(SQL);
        stmt.close();
	}
	
	
	// WARNING!! FOR SOME REASON, ATTEMPTING TO READ FROM THE GZIP FILE AND WRITE DIRECTLY TO THE DATABASE
	//   STORES ALL OF THE STRINGS INTO MEMORY FOR THE H2 PAGESTORE, AND THOSE STRINGS ARE ****NOT**** GARBAGE COLLECTED
	//   WHICH BLOWS THE HEAP!!!!!!!!!!!!!!!!!!!!!!!!
	// INSTEAD, USE THE OTHER METHODS TO WRITE THE KEY,POS PAIRS TO A TEMP TEXT FILE, THEN WRITE TO THE DATABASE.
	public void zipIndexesToDb(File bgzipFile, int keyColumn, boolean isKeyInteger, String delimiter, Connection dbConn) throws NumberFormatException, SQLException, IOException, InterruptedException {
		long fileLen = bgzipFile.length();
		BlockCompressedInputStream instr = new BlockCompressedInputStream(bgzipFile);
		
		String line = null;
		long numObjects = 0;
		long numBytesRead = 0;
		long pos = 0;
		long MB = 1024L * 1024L;
		boolean isFirstLineRead = false;
		
		final String SQL = "INSERT INTO Indexer (Key, FilePos) VALUES (?, ?)";
		PreparedStatement stmt = dbConn.prepareStatement(SQL);
		int numInBatch = 0;
		dbConn.setAutoCommit(true);
		

		
		do {
			if( isFirstLineRead )
				pos = instr.getFilePointer();
			line = instr.readLine();
			isFirstLineRead = true;
			if( line == null  ||  line.startsWith("#") )
				continue;
			
			numObjects++;
			numBytesRead += line.length() + 1;
				
			//String[] cols = line.split(delimiter);
			//String key = cols[keyColumn-1];
			String key = getCol(line, delimiter, keyColumn);
			if(isKeyInteger)
				stmt.setLong(1, Integer.valueOf(key));
			else
				stmt.setString(1, key);
			stmt.setLong(2, pos);
			//stmt.execute();
			System.out.println(key + "\t" + pos);
			dbConn.commit();

			if( numObjects % 10000 == 0 ) {
				System.out.println(key + "    " + numObjects 
						+ ", avgBytesPerItem: " + (numBytesRead / numObjects) 
						+ ", MBs read: " + (numBytesRead/MB) + ", Mem (MBs): " + (getMemoryUse()/MB));
			}
		} while( line != null );

		
		stmt.close();
		
		System.out.println("Num objects read: " + numObjects);
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
						+ ", MBs read: " + (numBytesRead/MB) + ", Mem (MBs): " + (getMemoryUse()/MB));
			}
		} while( line != null );

		fin.close();
		stmt.close();
		
		System.out.println("Num objects read: " + numObjects);
	}
	
	
	public void createDbTableIndex(Connection dbConn) throws SQLException {
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
		for(String id : idsToFind) {
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
		stmt.close();
		return key2posMap;
	}
	

}
