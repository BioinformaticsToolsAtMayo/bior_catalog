package edu.mayo.bior.indexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.jayway.jsonpath.JsonPath;

import net.sf.samtools.util.BlockCompressedInputStream;
import net.sf.samtools.util.BlockCompressedOutputStream;

public class IndexUtils {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	/**
	 * 
	 * @param bgzipFile
	 * @param keyColumn
	 * @param isKeyInteger Is the key an integer or a string?
	 * @param delimiter
	 * @param jsonPathToKey  Example: $.store.book[0].title   (If this is null or "", then the whole column will be used as the index key)
	 * @param tmpTxt
	 * @throws SQLException
	 * @throws IOException
	 */
	public void zipIndexesToTextFile(File bgzipFile, String delimiter, int keyColumn, String jsonPathToKey, File tmpTxt) throws SQLException, IOException {
		BlockCompressedInputStream instr = new BlockCompressedInputStream(bgzipFile);
		
		// Compile the JsonPath to make it faster and more reusable
		JsonPath jsonPath = null;
		if( jsonPathToKey != null && jsonPathToKey.length() > 0 )
			jsonPath = JsonPath.compile(jsonPathToKey);
		
		String line = null;
		FileOutputStream fout = new FileOutputStream(tmpTxt);
		long pos = 0;
		boolean isFirstLine = true;
		final int MB = 1024*1024;
		int numObjects = 0;
		do {
			if(! isFirstLine ) 
				pos = instr.getFilePointer();
			line = instr.readLine();
			isFirstLine = false;
			if( line == null  ||  line.startsWith("#") )
				continue;
			numObjects++;
			String[] cols = line.split(delimiter);
			String key = cols[keyColumn-1];
			
			if(jsonPathToKey != null && jsonPathToKey.length() > 0)
				key = jsonPath.read(key);

			fout.write( (key + "\t" + pos + "\n").getBytes() );

			if( numObjects % 10000 == 0 ) {
				System.out.println(key + "    " + numObjects + ", Mem (MBs): " + (getMemoryUse()/MB));
			}
		} while( line != null );
		fout.close();
		System.out.println("Num objects read: " + numObjects);
	}



	/** Get the lines from the bgzip file that match the indexes.
	 * Return a HashMap that maps the key to the list of lines returned
	 * @throws IOException */
	public HashMap<String,List<String>> getZipLinesByIndex(File bgzipFile, HashMap<String,List<Long>> indexes) throws IOException {
		BlockCompressedInputStream instr = new BlockCompressedInputStream(bgzipFile);
		HashMap<String,List<String>> linesOut = new HashMap<String,List<String>>();
		String line = null;
		SortedSet<String> sortedKeys = new TreeSet<String>(indexes.keySet());
		for(String id : sortedKeys) {
			List<Long> positions = indexes.get(id);
			
			List<String> linesForId = linesOut.get(id);
			if(linesForId == null) {
				linesForId = new ArrayList<String>();
				linesOut.put(id, linesForId);
			}
			
			for(Long pos : positions) {
				instr.seek(pos);
				line = instr.readLine();
				if(line != null)
					linesForId.add(line);
			}
		}
		instr.close();
		return linesOut;
	}
	
	public void writeLines(HashMap<String,List<String>> keyToLinesMap, File txtOutFile) throws IOException {
		FileOutputStream fout = new FileOutputStream(txtOutFile);
		SortedSet<String> sortedKeys = new TreeSet<String>(keyToLinesMap.keySet());
		for(String key : sortedKeys) {
			List<String> lines = keyToLinesMap.get(key);
			for(String line : lines) {
				fout.write( (key + "\t" + line + "\n").getBytes() );
			}
		}
		fout.close();
	}

	public void writeLines(List<String> lines, File txtOutFile) throws IOException {
		FileOutputStream fout = new FileOutputStream(txtOutFile);
		for(String line : lines) {
			if(line != null)
				fout.write( (line + "\n").getBytes() );
		}
		fout.close();
	}


	public long getMemoryUse() {
		long memoryUse = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		return memoryUse;
	}
	

	public List<String> loadLines(File file) throws IOException {
		double start = System.currentTimeMillis();
		BufferedReader fin = new BufferedReader(new FileReader(file));
		String line = null;
		List<String> lines = new ArrayList<String>();
		while( (line = fin.readLine()) != null ) {
			lines.add(line);
		}
		System.out.println("mem used: " + getMemoryUse());
		fin.close();
		double end = System.currentTimeMillis();
		System.out.println("runtime: " + (end-start)/1000.0);
		return lines;
	}

	
	public HashMap<String,List<Long>> loadIndexZip(File idxZip) throws IOException {
		HashMap<String,List<Long>> map = new HashMap<String,List<Long>>();
		BlockCompressedInputStream instream = new BlockCompressedInputStream(idxZip);
		String line = null;
		while( (line = instream.readLine()) != null ) {
			String[] split = line.split("\t");
			String key = split[0];
			String pos = split[1];
			List<Long> positions = map.get(key);
			if(positions == null) 
				positions = new ArrayList<Long>();
			positions.add(Long.valueOf(pos));
		}
		instream.close();
		return map;
	}
	
	private HashMap<String,List<Long>> loadIndexTxt(File indexFile) throws IOException {
		BufferedReader fin = new BufferedReader(new FileReader(indexFile));
		String line = null;
		HashMap<String,List<Long>> indexes = new HashMap<String,List<Long>>();
		while( (line = fin.readLine()) != null ) {
			String[] splits = line.split("\t");
			String key = splits[0];
			List<Long> positions = new ArrayList<Long>();
			for(int i=1; i < splits.length; i++)
				positions.add(Long.valueOf(splits[i]));
			indexes.put(key, positions);
		}
		fin.close();
		return indexes;
	}

	
	/** Faster method of finding a column than by doing a split.  Will not work on columns separated by variable # of spaces */
	public String getCol(String fullLine, String delimiter, int col) {
		int currCol = 1;
		int start = 0;
		int end = fullLine.indexOf(delimiter);
		if(end == -1)
			end = fullLine.length();
		while(start != -1) {
			if(col == currCol) 
				return fullLine.substring(start, end);
			start = end + 1;
			end = fullLine.indexOf(delimiter, start);
			if(end == -1)
				end = fullLine.length();
			currCol++;
		}
		return null;
	}
	
	
	
	/** Find the next line within a bunch of text */
	public String getNextLine(String str, int start) {
		while(str.charAt(start) != '\n' && start < str.length()-1) {
			start++;
		}
		int end = start+1;
		while(str.charAt(end) != '\n' && end < str.length()) {
			end++;
		}
		return str.substring(start, end).trim();
	}

	public List<String> getLines(String str) {
		List<String> lines = new ArrayList<String>();
		int startIdx = 0;
		boolean isEnd = false;
		while( ! isEnd ) {
			String line = getNextLine(str, startIdx);
			if(line.length() == 0)
				break;
			lines.add(line);
			startIdx += line.length();
		}
		return lines;
	}

	
	/** Zip a file */
	public void bgzip(File txtFile, File bgzipOutFile) throws IOException {
		BlockCompressedOutputStream outstream = new BlockCompressedOutputStream(bgzipOutFile, 9);
		
		byte[] buf = new byte[64*1024];
		FileInputStream fin = new FileInputStream(txtFile);
		int len = -1;
		while( (len = fin.read(buf)) != -1 ) {
			outstream.write(buf, 0, len);
		}
		fin.close();
		outstream.close();
	}
	
	/** Save indexes to a zipped file (tab-delimited key,position pairs) */ 
	private void saveIndexZip(HashMap<String,List<Long>> indexes, File indexFile) throws IOException {
		BlockCompressedOutputStream fout = new BlockCompressedOutputStream(indexFile);
		for(String key : indexes.keySet()) {
			fout.write( (key + "\t" + indexes.get(key) + "\n").getBytes() );
		}
		fout.close();
	}



	
	public List<String> getSampleGeneIds() {
		return Arrays.asList(
				"uc003bmz.4",
				"uc003bng.3",
				"uc010gsn.3",
				"uc010gvh.1",
				"uc010gyg.3",
				"uc010hat.1",
				"uc010hau.1",
				"uc011ahw.2",
				"uc011akb.1",
				"uc011amk.2",
				"uc011aou.2",
				"uc011aqb.1",
				"uc011arw.2",
				"uc021wku.1",
				"uc021wlx.1",
				"uc021wmk.1",
				"uc021wod.1",
				"uc021wox.1",
				"uc021wpn.1",
				"uc021wqd.1",
				"uc021wqx.1",
				"uc021wqy.1",
				"uc021wsf.1"
				);
	}


	public List<String> getSampleVariantIds() {
		return Arrays.asList(
				"rsX",
				"rs149201999",
				"rs62224621",
				"rs12162831",
				"rs143158770",
				"rs146675715",
				"rs187630378",
				"rs149862772",
				"rs187249052",
				"rs190654308",
				"rs117120972",
				"rs186426805"
				
				);
	}
}
