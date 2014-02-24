/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.utils;

import edu.mayo.pipes.JSON.inject.ColumnInjector;
import edu.mayo.pipes.JSON.inject.Injector;
import edu.mayo.pipes.JSON.inject.JsonType;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author dquest
 */
public class SQLParser {
    
    List<String> isFieldLineMatches = null;
    
    public SQLParser(){
        //don't initialize any fields
        isFieldLineMatches = new ArrayList<String>();
    }
    
    /**
     * 
     * @param isFieldLine a set of strings for identifying if it is a field line, e.g. VARCHAR, DATE, INT, NUMBER
     */
    public SQLParser(List<String> isFieldLine){
        isFieldLineMatches = isFieldLine;
    }
    
    private void initDefaults(){
        isFieldLineMatches = new ArrayList<String>();
        isFieldLineMatches.add("VARCHAR");
        isFieldLineMatches.add("CHAR"); 
        isFieldLineMatches.add("DATE");
        isFieldLineMatches.add(" INT");
        isFieldLineMatches.add(" SMALLINT ");
        isFieldLineMatches.add("DECIMAL");
        isFieldLineMatches.add("ENUM");
        isFieldLineMatches.add(" TEXT");
        isFieldLineMatches.add(" YEAR");        
    }
    
    public SQLParser(boolean useIsFieldLineDefaults){
        initDefaults();
    }
    
    //cat *.sql | grep "NOT NULL" | grep -v char | grep -v blob | grep -v int | grep -v float | grep -v double | grep -v date | grep -v enum | grep -v text | grep -v set | grep -v time | less
    public JsonType getType(String line){
        //return JsonType.STRING;
        String field = getField4Line(line);
        //System.out.println(line);
        //System.out.println(field);
        if(field == null){
            //do the safe thing...
            return JsonType.STRING;
        }
        if(field.equalsIgnoreCase("chrom")){
            return JsonType.STRING;
        }
        if(line.contains("enum(")){
            return JsonType.STRING;
        }
        if(line.contains("set(")){
            return JsonType.STRING;
        }     
        
        if(line.contains("int") && !field.contains("int")){
            //int
            return JsonType.NUMBER;
        }else if((line.contains("float")||line.contains("double")) && !field.contains("int")){
            //float
            //double
            return JsonType.NUMBER;
        }else {//all these types are Strings (at least for now)
            //grep -v char 
            //blob/longblob 
            //date 
            //enum 
            //text 
            //set 
            //time
            return JsonType.STRING;
        }
    }
    //needed improvement, check the dialect and build this based on the SQL dialect
    public boolean isFieldLine(String line){
        if(line.matches(".*`.*`.*NOT NULL.*")) return true;
        if(line.matches(".*`.*`.*DEFAULT NULL.*")) return true;
        if(line.matches(".*`.*`.*default NULL.*")) return true;
        if(line.matches(".*`.*`.*blob,.*")) return true;
        for(String opt : isFieldLineMatches){
            if(line.contains(opt)){
                return true;
            }
        }
        return false;
    }
    
    public String getField4Line(String line){
        String[] split = line.split("`");
        // Ex: "  `chrom` varchar(255) NOT NULL,";
        // OR  "  KEY `chrom` (`chrom`(14),`bin`)"
        if( split.length >= 3 ) {
            return split[1];
        }
        //Ex:	"journal VARCHAR(10),"
        else {
            split = line.split("\\s+");
            return split[1];
        }
    }
    
    /**
     * e.g. convert CREATE TABLE journal ( into journal
     * @param line
     * @return 
     */
    public String getTableName(String line){
        String name = "";
        if(!line.contains("CREATE TABLE")){
            return name;
        }else {
            String tmp = line.replaceAll("CREATE TABLE", "");
            tmp = tmp.replaceAll("\\(", "");
            name = tmp.trim();
        }
        return name;
    }
    
    public int getCreateLine(List<String> lines){
        return getCreateLine(lines, 0);
    }
    
    /**
     * starting at startpos, get the first create line you can find.
     * @param lines
     * @param startPos
     * @return 
     */
    public int getCreateLine(List<String> lines, int startPos){
        int n = 0;
        for(int i=startPos; i<lines.size(); i++ ){
            if(lines.get(i).contains("CREATE TABLE")){
                return i;
            }
        }
        return n;
    }
    
    public int getCloseLine(List<String> lines){
        return getCloseLine(lines, 0);
    }
    
    //) ENGINE = MyISAM CHARSET=utf8
    /**
     * Starting at startpos, get the first close line you can find and return the line number.
     * @param lines
     * @param startPos
     * @return 
     */
    public int getCloseLine(List<String> lines, int startPos){
        int n = 0;
        for(int i=startPos; i<lines.size(); i++ ){
            if(lines.get(i).matches(".*\\) ENGINE.MyISAM .*DEFAULT CHARSET.*") 
                    || lines.get(i).matches(".*\\) ENGINE.*MyISAM.*CHARSET.*") ){
                return i;
            }
        }
        return n;
    }
    
    
    public ColumnInjector makeInjector(int column, String columnName, JsonType type){
        //System.out.println(column + ":" + columnName + ":" + type.toString());
        return new ColumnInjector(column, columnName, type);       
    }
    
    public ColumnInjector[] getInjectorsFromSQL(List<String> lines){
        int count = 0;         
        int createLine = this.getCreateLine(lines);
        int closeLine = this.getCloseLine(lines);
        ArrayList<ColumnInjector> jects = new ArrayList<ColumnInjector>();
        for(int i=createLine+1; i<closeLine; i++){
            String line = lines.get(i);
            if(isFieldLine(line)){
                count++;
                System.out.println(line);
                jects.add( makeInjector(count,getField4Line(line), getType(line)) );
            }
        }
        ColumnInjector[] array = jects.toArray(new ColumnInjector[jects.size()]);
        return array;
    }
    
    /**
     * 
     * @param file - the complete path to the SQL file
     * @return Table,<Field,ColumnInjector> - Key is the table name, value is an injector that holds the variable name and type
     */
    public HashMap<String,HashMap<String,ColumnInjector>> getInjectorsFromSQLFile(String file, boolean reporting) throws IOException{
        List<String> lines = loadFileToMemory(file);
        HashMap<String,HashMap<String,ColumnInjector>> schema = new HashMap<String,HashMap<String,ColumnInjector>>();

        int createLine = this.getCreateLine(lines);
        int closeLine = this.getCloseLine(lines);
        String tableName = this.getTableName(lines.get(createLine));
        int count = 0;
        while(closeLine > 0 ){
            HashMap<String,ColumnInjector> injectors = new HashMap<String,ColumnInjector>();
            if(reporting )System.out.println(tableName);
            for(int i=createLine+1; i<closeLine; i++){
                String line = lines.get(i);
                if(isFieldLine(line)){
                    count++;
                    if(reporting) System.out.println("\t" + line);
                    String field = getField4Line(line);
                    if(reporting) System.out.println("****" + field);
                    injectors.put(field, makeInjector(count,getField4Line(line), getType(line)));
                }
            }
            //save to hash
            schema.put(tableName,injectors);

            //move on to the next table description
            createLine = this.getCreateLine(lines, closeLine+1);
            closeLine = getCloseLine(lines, closeLine+1);
            tableName = this.getTableName(lines.get(createLine));
            count = 0;
        }
        return schema;
    }
    

    
    public String getGoldenAttr(String line){
        String field = getField4Line(line);
        if(field.equalsIgnoreCase("chromStart")){
            return CoreAttributes._minBP.toString();
        }else if(field.equalsIgnoreCase("chromEnd")){
            return CoreAttributes._maxBP.toString();
        }
        //txStart - reflat
        else if(field.equalsIgnoreCase("txStart")){
            return CoreAttributes._minBP.toString();
        }
        //txEnd   - reflat
        else if(field.equalsIgnoreCase("txEnd")){
            return CoreAttributes._maxBP.toString();
        }
        
        //tName":"chr1",
        else if(field.equalsIgnoreCase("tName")){
            return CoreAttributes._landmark.toString();
        }
        //tStart":"879582","
        else if(field.equalsIgnoreCase("tStart")){
            return CoreAttributes._minBP.toString();
        }
        //tEnd":"894631"
        else if(field.equalsIgnoreCase("tEnd")){
            return CoreAttributes._maxBP.toString();
        }
        
        if(field.equalsIgnoreCase("chrom")){
            return CoreAttributes._landmark.toString();
        }

        //endStart
        if(field.equalsIgnoreCase("endStart")){
            return CoreAttributes._maxBP.toString();
        }
        
        //genoName
        if(field.equalsIgnoreCase("genoName")){
            return CoreAttributes._landmark.toString();
        }
        
        //genoStart
        if(field.equalsIgnoreCase("genoStart")){
            return CoreAttributes._minBP.toString();
        }
        
        //genoEnd
        if(field.equalsIgnoreCase("genoEnd")){
            return CoreAttributes._maxBP.toString();
        }
        
        return null;
    }
    
    public String[] getGoldenDrillPaths(List<String> lines, boolean reporting){
        String landmark = "super.cala.fragalistic.expi.ali.never.match";
        String minbp = "super.cala.fragalistic.expi.ali.never.match";
        String maxbp = "super.cala.fragalistic.expi.ali.never.match";
        int createLine = this.getCreateLine(lines);
        int closeLine = this.getCloseLine(lines);
        for(int i=createLine+1; i<closeLine; i++){ 
            if(this.isFieldLine(lines.get(i))){
                if(reporting) System.out.println(": " + lines.get(i));
                if(getGoldenAttr(lines.get(i)) != null ){
                    String attr = getGoldenAttr(lines.get(i));                    
                    //what golden is it?
                    //_landmark
                    if(attr.equalsIgnoreCase(CoreAttributes._landmark.toString())){
                        //System.out.println("    Landmark: " + lines.get(i));
                        landmark = getField4Line(lines.get(i));
                        if(reporting) System.out.println("    Drill Landmark: " + landmark);
                    }
                    //_minBP
                    if(attr.equalsIgnoreCase(CoreAttributes._minBP.toString())){
                        //System.out.println("    minBP: " + lines.get(i));
                        minbp = getField4Line(lines.get(i));
                        if(reporting) System.out.println("    Drill minBP: " + minbp);
                    }
                    //_maxBP
                    if(attr.equalsIgnoreCase(CoreAttributes._maxBP.toString())){
                        //System.out.println("    maxBP: " + lines.get(i));
                        maxbp = getField4Line(lines.get(i));
                        if(reporting) System.out.println("    Drill maxBP: " + maxbp);
                    }
                }
            }
        }
        
        String[] ret = new String[] {landmark, minbp, maxbp};
        return ret;
        
    }
    
    public void semanticErrorOut(String goldenAttributeName, List<String> lines){
        
    }
    
    public List<String> loadFileToMemory(String sqlfile) throws IOException{
        BufferedReader br = null;
        List<String> file = new ArrayList<String>();
        //StringBuilder sb = new StringBuilder();
        try {
            br = new BufferedReader(new FileReader(sqlfile));
            String line = br.readLine();
            while (line != null) {
                //sb.append(line);
                //sb.append("\n");
                //System.out.println(line);
                file.add(line);
                line = br.readLine();
            }

        } finally {
            br.close();
        }
        return file;
    }
    
    private ColumnInjector getInjectorForLine(int lineNumber, String line){
        String id = getField4Line(line);
        if(id==null){
            return null;
        }
        JsonType jtype = getType(line);
        return new ColumnInjector(lineNumber,id,jtype);
    }
    
    public int[] cutArr(int size){
        int[] c = new int[size];
        for(int i=0;i<size;i++){
            c[i]=i+1;
            //System.out.println(c[i]);
        }
        return c;
    }
    
}
