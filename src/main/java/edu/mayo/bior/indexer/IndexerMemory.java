package edu.mayo.bior.indexer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import net.sf.samtools.util.BlockCompressedInputStream;
import net.sf.samtools.util.BlockCompressedOutputStream;

public class IndexerMemory extends IndexUtils {
	public static void main(String[] args) {
		try {
			IndexerMemory zip = new IndexerMemory();
			
			File idxTxt = new File("resources/idxSorted.idx.txt");
			File idxZip = new File("resources/idxSorted.idx.gz");
			zip.bgzip(idxTxt, idxZip);
			double start = System.currentTimeMillis();
			zip.loadIndexZip(idxZip);
			double end = System.currentTimeMillis();
			System.out.println("runtime = " + (end-start)/1000.0);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	public void testVariants() throws IOException {
		File bgzipFile = new File("resources/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz");
		File indexFileOut = new File("resources/dbsnp.chr22.indexByRsid.idx.gz");
		File queryResultTxt = new File("resources/queryResults.variants.txt");

		// Given a set of sample keys / ids, get the rows from the bgzip file and save to txt file
		List<String> sampleKeys = getSampleVariantIds();
		HashMap<String,List<Long>> allIndexes  = loadIndexZip(indexFileOut);
		HashMap<String,List<Long>> matchingIndexes = findIndexes(sampleKeys, allIndexes);
		List<String> lines = getLinesByIndex(bgzipFile, matchingIndexes);
		writeLines(lines, queryResultTxt);
	}
	
	public void testGenes() throws IOException {
		File bgzipFile = new File("resources/knowngene.txt.gz");
		File indexFileOut = new File("resources/knowngene.indexByGeneId.idx.gz");
		File queryResultTxt = new File("resources/queryResults.genes.txt");

		// Given a set of sample keys / ids, get the rows from the bgzip file and save to txt file
		List<String> sampleKeys = getSampleGeneIds();
		HashMap<String,List<Long>> allIndexes = loadIndexZip(indexFileOut);
		HashMap<String,List<Long>> matchingIndexes = findIndexes(sampleKeys, allIndexes);
		List<String> lines = getLinesByIndex(bgzipFile, matchingIndexes);
		writeLines(lines, queryResultTxt);
	}
	
	
	
	public HashMap<String,List<Long>> findIndexes(List<String> idsToFind, HashMap<String,List<Long>> allIndexes) {
		HashMap<String,List<Long>> matchingIndexes = new HashMap<String,List<Long>>();
		for(String id : idsToFind) {
			List<Long> positions = allIndexes.get(id);
			if(positions != null)
				matchingIndexes.put(id, positions);
		}
		return matchingIndexes;
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
}
