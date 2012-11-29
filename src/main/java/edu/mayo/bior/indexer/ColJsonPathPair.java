package edu.mayo.bior.indexer;

/** Represents a column,jsonPath pair, where
 *   column is an integer referring to the column where the desired data can be found, and
 *   jsonPath is the jsonPath string to the value within that column's json string to extract.
 *   If there is no json in the column, or the whole column is what you want, then use null for jsonPath
 * @author m054457
 *
 */
public class ColJsonPathPair {
	public int column = -1;
	public String jsonPath = null;
	
	public ColJsonPathPair(int col, String json) {
		column = col;
		jsonPath = json;
	}
	
	/** Split a column:jsonPath pair up into the column int and jsonPath string 
	 * TODO: warning jsonpath can contain : characters which might mess with the splits 
	 *   see: http://goessner.net/articles/JsonPath/*/
	public  ColJsonPathPair(String colJsonPair) {
		if(IndexUtils.isInteger(colJsonPair))
			column = Integer.valueOf(colJsonPair);
		else {
			String[] parts = colJsonPair.split(":");
			column = Integer.valueOf(parts[0]);
			jsonPath = parts[1];
		}
	}

	public String toString() {
		String jsonPart = "";
		if( jsonPath != null && jsonPath.length() > 0 )
			jsonPart = ":" + jsonPath;
		return column + jsonPart;
	}
}
