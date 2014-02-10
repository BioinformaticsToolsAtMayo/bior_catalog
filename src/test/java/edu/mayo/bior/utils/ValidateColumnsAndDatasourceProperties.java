package edu.mayo.bior.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;


/**
 * @author Michael Meiners (m054457)
 * Date created: Sep 4, 2013
 */
public class ValidateColumnsAndDatasourceProperties {

	// this should be REMOVED and instead replaced with the Type class from ColumnMetaData
	public enum Type { JSON, String, Float, Integer, Boolean };

	
	public static void main(String[] args) {
		try {
			String startDir = "/Volumes/data5/bsi/catalogs/bior/v1/";
			new ValidateColumnsAndDatasourceProperties().validateColumnsAndDatasourcesPropertiesFiles(startDir);
			System.out.println("Done");
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void validateColumnsAndDatasourcesPropertiesFiles(String startingDir) throws IOException {
		List<File> allPropsFiles = getAllPropsFiles(new File(startingDir));
		System.out.println("Number of files to validate: " + allPropsFiles.size());
		
		// Go thru each properties file and validate
		for(File file : allPropsFiles) {
			if(file.getName().endsWith(".columns.tsv"))
				validateCols(file);
			else if( file.getName().endsWith(".datasource.properties") )
				validateDatasource(file);
		}
	}

	private void validateDatasource(File file) {
		// TODO: Not implemented yet!!!!!!!!!!!!
	}

	private void validateCols(File file) throws IOException {
		List<String> lines = FileUtils.readLines(file);
		int numBad = 0;
		for(int i = 0; i < lines.size(); i++) {
			String line = lines.get(i);
			if( ! line.startsWith("#") && line.trim().length() > 0 ) {
				boolean isValid = validateColLine(file, line, i+1);
				if( ! isValid)
					numBad++;
			}
		}
		if(numBad > 0)
			System.out.println("Number of bad lines in file: " + numBad + "\n----------------------");
	}

	private boolean validateColLine(File file, String line, int lineNum) throws IOException {
		String[] parts = line.split("\\t");
		if(parts.length != 4) {
			System.out.println(file.getCanonicalPath() + "\nLine does not have 4 columns: (line " + lineNum + ") : \n    " + line);
			for(int i=0; i < parts.length; i++)
				System.out.println("      " + (i+1) + ") " + parts[i]);
			return false;
		}
		if( ! isCorrectType(parts[1]) ) {
			System.out.println(file.getCanonicalPath() + "\nType is not correct: (line " + lineNum + ") : \n    " + line);
			return false;
		}
		if( ! isCorrectCount(parts[2]) ) {
			System.out.println(file.getCanonicalPath() + "\nNumber/Count is not correct: (line " + lineNum + ") : \n    " + line);
			return false;
		}
		if( parts[3].trim().length() == 0 ) {
			System.out.println(file.getCanonicalPath() + "\nNo description is given: (line " + lineNum + ") : \n    " + line);
			return false;
		}
		return true;
	}
	
	private boolean isCorrectType(String type) {
		for(Type t : Type.values()) {
			if(t.toString().equals(type))
				return true;
		}
		return false;
	}
	
	private boolean isCorrectCount(String count) {
		return count.equals(".") || count.equals("0") || count.equals("1");
	}

	private List<File> getAllPropsFiles(File startingDirOrFile) {
		List<File> fileList = new ArrayList<File>();
		for(File file : startingDirOrFile.listFiles()) {
			if( file.isDirectory() )
				fileList.addAll(getAllPropsFiles(file));
			else if(isPropsFile(file))
				fileList.add(file);
		}
		return fileList;
	}

	private boolean isPropsFile(File file) {
		return file.isFile() && (file.getName().endsWith(".columns.tsv") || file.getName().endsWith(".datasource.properties"));
	}

}
