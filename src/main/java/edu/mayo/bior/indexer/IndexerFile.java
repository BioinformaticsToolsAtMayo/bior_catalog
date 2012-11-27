package edu.mayo.bior.indexer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.code.externalsorting.ExternalSort;

import net.sf.samtools.util.BlockCompressedInputStream;
import net.sf.samtools.util.BlockCompressedOutputStream;

public class IndexerFile extends IndexUtils {

	// TODO:  Make one to many Id->Positions in case Id is NOT unique

	
	
	public static void main(String[] args) {
		try {
			//System.setProperty("jzran.zlib.file", "/usr/lib/libz.dylib");
			IndexerFile zip = new IndexerFile();
			//zip.testGenes();
			
			//zip.testVariants();
			zip.searchOnly();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	public void testVariants() throws IOException, SQLException {
		File bgzipFile = new File("resources/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz");
		//File vcfFile   = new File("resources/1000g.partial.vcf");
		//File bgzipFile = new File("resources/1000g.partial.vcf.gz");
		File indexFileUnsorted = new File("resources/idxUnsorted.idx.txt");
		File indexFileSorted   = new File("resources/idxSorted.idx.txt");
		File queryResultTxt = new File("resources/queryResults.variants.txt");

		// Bgzip the test vcf file
		//bgzip(vcfFile, bgzipFile);
		
		// Save ALL indexes, separated by tabs, based on rsId in column 2 (0-based)
		System.out.println("Creating indexes...");
		zipIndexesToTextFile(bgzipFile, "\t", 3, null, indexFileUnsorted);
		System.out.println("Sorting index file...");
		SortExternal.sortIndexFile(indexFileUnsorted, indexFileSorted, false);

		
		// Given a set of sample keys / ids, get the rows from the bgzip file and save to txt file
		List<String> sampleKeys = getSampleVariantIds();
		System.out.println("Finding sample variant ids...");
		HashMap<String,List<Long>> matchingIndexes = findIndexes(sampleKeys, indexFileSorted);
		List<String> lines = getLinesByIndex(bgzipFile, matchingIndexes);
		System.out.println("Writing results to output txt file...");
		writeLines(lines, queryResultTxt);
		System.out.println("DONE.");
	}
	
	
	private void searchOnly() throws IOException {
		double start = System.currentTimeMillis();
		File bgzipFile = new File("resources/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz");
		File indexFileSorted   = new File("resources/idxSorted.idx.txt");
		File queryResultTxt = new File("resources/queryResults.variants.txt");

		// Given a set of sample keys / ids, get the rows from the bgzip file and save to txt file
		List<String> sampleKeys = getSampleVariantIds();
		System.out.println("Finding sample variant ids...");
		HashMap<String,List<Long>> matchingIndexes = findIndexes(sampleKeys, indexFileSorted);
		List<String> lines = getLinesByIndex(bgzipFile, matchingIndexes);
		System.out.println("Writing results to output txt file...");
		writeLines(lines, queryResultTxt);
		double end = System.currentTimeMillis();
		System.out.println("DONE.  Runtime = " + (end-start)/1000.0);
	}
	

	/** Get the lines from the bgzip file that match the indexes 
	 * @throws IOException */
	private List<String> getLinesByIndex(File bgzipFile, HashMap<String,List<Long>> indexes) throws IOException {
		BlockCompressedInputStream instr = new BlockCompressedInputStream(bgzipFile);
		List<String> linesOut = new ArrayList<String>();
		String line = null;
		SortedSet<String> sortedKeys = new TreeSet<String>(indexes.keySet());
		for(String id : sortedKeys) {
			List<Long> positions = indexes.get(id);
			for(Long pos : positions) {
				instr.seek(pos);
				line = instr.readLine();
				if(line != null)
					linesOut.add(line);
			}
		}
		instr.close();
		return linesOut;
	}

	
	public HashMap<String,List<Long>> findIndexes(List<String> idsToFind, File idxFileSorted) throws IOException {
		HashMap<String,List<Long>> matchingIndexes = new HashMap<String,List<Long>>();
		RandomAccessFile idxFileRnd = new RandomAccessFile(idxFileSorted, "r");
		for(String id : idsToFind) {
			if(matchingIndexes.keySet().contains(id)) 
				continue;
			
			List<Long> positions = getPositionsForId(id, idxFileRnd);
			matchingIndexes.put(id, positions);
		}
		return matchingIndexes;
	}
	
	private List<Long> getPositionsForId(String id, RandomAccessFile idxFileRnd) throws IOException {
		long searchPos = search(idxFileRnd, id);
		
		// If the search position is off the end of the file, then we did not find what we were looking for
		long fileLen = idxFileRnd.length();
		if(searchPos >= fileLen) 
			return new ArrayList<Long>();
		
		long firstPos = searchPos;
		
		final long BLOCK_SIZE = 512; 
		// Only go backwards if firstPos is NOT the beginning of the file
		if( firstPos != 0 ) {
			while(true) {
				firstPos -= BLOCK_SIZE;
				if(firstPos < 0) {
					firstPos = 0;
					break;
				}
				if(! containsId(id, getBlock(idxFileRnd, firstPos, BLOCK_SIZE)) ) {
					firstPos += BLOCK_SIZE;
					break;
				}
			}
		}
		
		idxFileRnd.seek(firstPos);
		
		// Now, starting at firstPos, get all lines going forward
		// If we are not at the 0th index within the file, then we may be in the middle of the line, so strip off first part of line
		if( firstPos != 0 && (firstPos != searchPos))
			idxFileRnd.readLine();
		List<Long> filePositions = new ArrayList<Long>();
		boolean isHitFirstMatch = false;
		boolean isFirstMissAfterHit = false;
		String line = "";
		while( ! isFirstMissAfterHit && line != null) {
			line = idxFileRnd.readLine();
			if( containsId(id, line) )  { 
				filePositions.add(getPos(line));
				isHitFirstMatch = true; 
			} else if( isHitFirstMatch ) {
				isFirstMissAfterHit = true;
			}
		}
		return filePositions;
	}

	/** Get the FilePosition part of the key/value pair line*/
	private Long getPos(String line) {
		return Long.valueOf(line.substring(line.indexOf("\t") + 1));
	}
	
	private String[] getBlock(RandomAccessFile idxFileRnd, long pos, long blockSize) throws IOException {
		long lastPos = pos - blockSize;
		if(lastPos < (0-blockSize))
			return new String[0];
		else if(lastPos < 0) 
			lastPos = 0;
		byte[] buf = new byte[(int)blockSize];
		int len = idxFileRnd.read(buf);
		String blockStr = new String(buf, 0, len);
		
		// If not at 0th position, then remove first line since it is a partial
		if( lastPos != 0 ) {
			int idxNewline = blockStr.indexOf("\n");
			if(idxNewline != -1)
				blockStr = blockStr.substring(idxNewline);
		}
			
		return blockStr.split("\n");
	}
	
	private boolean containsId(String id, String[] lines) {
		for(String line : lines) {
			if(containsId(id, line)) 
				return true;
		}
		return false;
	}
	
	private boolean containsId(String id, String line) {

		if(line == null || line.length() == 0 || line.indexOf("\t") == -1)
			return false;
		int idx = line.indexOf("\t");
		String idFromLine = line.substring(0, idx);
		return id.equals(idFromLine);
	}
	
	/**
	 * Find the position of the start of the first line in the file that is
	 * greater than or equal to the target line, using a binary search.
	 * 
	 * @param file
	 *            the file to search.
	 * @param target
	 *            the target to find.
	 * @return the position of the first line that is greater than or equal to
	 *         the target line.
	 * @throws IOException
	 * See:  http://blog.sarah-happy.ca/2010/04/binary-search-of-sorted-text-file.html
	 */
	private long search(RandomAccessFile file, String target)   throws IOException {
	    /*
	     * because we read the second line after each seek there is no way the
	     * binary search will find the first line, so check it first.
	     */
	    file.seek(0);
	    String line = file.readLine();
	    if (line == null || line.compareTo(target) >= 0) {
	        /*
	         * the start is greater than or equal to the target, so it is what
	         * we are looking for.
	         */
	        return 0;
	    }

	    /*
	     * set up the binary search.
	     */
	    long beg = 0;
	    long end = file.length();
	    while (beg <= end) {
	        /*
	         * find the mid point.
	         */
	        long mid = beg + (end - beg) / 2;
	        file.seek(mid);
	        file.readLine();
	        line = file.readLine();

	        if( line == null || line.length() == 0) {
	        	end = mid - 1;
	        	continue;
	        }
	        
	        String id = line.substring(0, line.indexOf("\t"));
	        if (id.compareTo(target) >= 0) {
	            /*
	             * what we found is greater than or equal to the target, so look
	             * before it.
	             */
	            end = mid - (line.length());
	        } else {
	            /*
	             * otherwise, look after it.
	             */
	            beg = mid + (line.length());
	        }
	    }

	    /*
	     * The search falls through when the range is narrowed to nothing.
	     */
	    file.seek(beg);
	    file.readLine();
	    return file.getFilePointer();
	}
}
