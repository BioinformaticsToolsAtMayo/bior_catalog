package edu.mayo.bior.indexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import net.sf.samtools.util.BlockCompressedInputStream;

public class IndexerMemory  {
	
	private IndexUtils utils = new IndexUtils();
	
	public static void main(String[] args) {
		try {
			IndexerMemory zip = new IndexerMemory();
			IndexUtils utils = new IndexUtils();
			File idxTxt = new File("resources/idxSorted.idx.txt");
			File idxZip = new File("resources/idxSorted.idx.gz");
			utils.bgzip(idxTxt, idxZip);
			double start = System.currentTimeMillis();
			utils.loadIndexBgzip(idxZip);
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
		List<String> sampleKeys = utils.getSampleVariantIds();
		HashMap<String,List<Long>> allIndexes  = utils.loadIndexBgzip(indexFileOut);
		HashMap<String,List<Long>> matchingIndexes = findIndexes(sampleKeys, allIndexes);
		List<String> lines = getLinesByIndex(bgzipFile, matchingIndexes);
		utils.writeLines(lines, queryResultTxt);
	}
	
	public void testGenes() throws IOException {
		File bgzipFile = new File("resources/knowngene.txt.gz");
		File indexFileOut = new File("resources/knowngene.indexByGeneId.idx.gz");
		File queryResultTxt = new File("resources/queryResults.genes.txt");

		// Given a set of sample keys / ids, get the rows from the bgzip file and save to txt file
		List<String> sampleKeys = utils.getSampleGeneIds();
		HashMap<String,List<Long>> allIndexes = utils.loadIndexBgzip(indexFileOut);
		HashMap<String,List<Long>> matchingIndexes = findIndexes(sampleKeys, allIndexes);
		List<String> lines = getLinesByIndex(bgzipFile, matchingIndexes);
		utils.writeLines(lines, queryResultTxt);
	}
	
	
	
	public HashMap<String,List<Long>> findIndexes(List<String> idsToFind, HashMap<String,List<Long>> allIndexes) {
		HashMap<String,List<Long>> matchingIndexes = new HashMap<String,List<Long>>();
		int count = 0;
		// Remove any duplicate ids by assigning to a HashSet
		double start = System.currentTimeMillis();
		Set<String> idSet = new HashSet<String>(idsToFind);
		Iterator<String> iter = idSet.iterator();
		double end = System.currentTimeMillis();
		System.out.println("Time to create set from list: " + (end-start)/1000.0);
		System.out.println("Set size: " + idSet.size());
		int maxMem = 0;
		while(iter.hasNext()) {
			String id = iter.next();
			count++;
			if(count % 100000 == 0 ) {
				double now = System.currentTimeMillis();
				int numPerSec = (int)(count/((now-end)/1000.0));
				long mem = utils.getMemoryUseMB();
				if(mem > maxMem)
					maxMem = (int)mem;
				System.out.println(count + "\t #/sec: " + numPerSec + "\t Est time: " + ((idSet.size()-count)/numPerSec) + " s  " + mem + "MB");
			}
			List<Long> positions = allIndexes.get(id);
			matchingIndexes.put(id, positions);
		}
		System.out.println("Max memory: " + maxMem);

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
