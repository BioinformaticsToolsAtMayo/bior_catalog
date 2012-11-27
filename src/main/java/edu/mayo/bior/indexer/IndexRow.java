package edu.mayo.bior.indexer;

import java.util.Comparator;

public class IndexRow implements Comparable<IndexRow> {
	public String key;
	public long filePos;
	
	
	public IndexRow(String key, long filePos) {
		this.key = key;  
		this.filePos = filePos;
	}
	
	public static Comparator keyToIndexComparator = new Comparator() {
		@Override
		public int compare(Object idxRow, Object key) {
			return ((IndexRow)idxRow).key.compareToIgnoreCase((String)key);
		}
	};

	@Override
	public int compareTo(IndexRow other) {
		return this.key.compareTo( ((IndexRow)other).key );
	}

	
}
