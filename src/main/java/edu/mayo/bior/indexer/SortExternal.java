package edu.mayo.bior.indexer;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import com.google.code.externalsorting.ExternalSort;


/** Sort a text file without bringing it into memory */
public class SortExternal {

	private static Comparator<String> stringKeyComparator = new Comparator<String>() {
        public int compare(String r1, String r2){
        	String key1 = r1.split("\t")[0];
        	String key2 = r2.split("\t")[0];
        	return key1.compareTo(key2);
        }
    };

	private static Comparator<String> intKeyComparator = new Comparator<String>() {
        public int compare(String r1, String r2){
        	Long key1 = Long.valueOf(r1.split("\t")[0]);
        	Long key2 = Long.valueOf(r2.split("\t")[0]);
        	return key1.compareTo(key2);
        }
    };


	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	/** Should account for integer columns and only sorting on key columns */
	public static void sortIndexFile(File indexFileUnsorted, File indexFileSorted, boolean isKeyInteger) throws IOException {
        Comparator<String> comparator = isKeyInteger ? intKeyComparator : stringKeyComparator;
        List<File> fileBlocks = ExternalSort.sortInBatch(indexFileUnsorted, comparator) ;
        ExternalSort.mergeSortedFiles(fileBlocks, indexFileSorted, comparator);
    }
}
