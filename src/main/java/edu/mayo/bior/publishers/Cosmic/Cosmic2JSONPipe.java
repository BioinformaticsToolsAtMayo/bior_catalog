package edu.mayo.bior.publishers.Cosmic;

import java.util.List;
import java.util.NoSuchElementException;

import com.google.gson.JsonObject;
import com.tinkerpop.pipes.AbstractPipe;

public class Cosmic2JSONPipe extends AbstractPipe<String,String> {
	
    String chr = "UNKNOWN";
    String positions = "UNKNOWN";
    String alleles = "UNKNOWN";
    List<String> headerColumns = null;

    public Cosmic2JSONPipe(String chr, 
    						String positions, 
    						String alleles,
    						List<String> headerColumns) {
		this.chr = chr;
		this.positions = positions;
		this.alleles = alleles;
		this.headerColumns = headerColumns;
	}

	@Override
	protected String processNextStart() throws NoSuchElementException {
		String s = this.starts.next();
		
		String[] split = s.split(" ");
        if(headerColumns.size() == split.length){
            JsonObject record = new JsonObject();
            for(int i=0;i<split.length;i++){
                record.addProperty(headerColumns.get(i), split[i]);
            }
            return record.toString();
        } else {
            throw new NoSuchElementException();
        }
	}
}
