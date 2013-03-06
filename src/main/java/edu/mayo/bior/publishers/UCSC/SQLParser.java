/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.UCSC;

import edu.mayo.pipes.JSON.inject.ColumnInjector;
import edu.mayo.pipes.JSON.inject.Injector;
import edu.mayo.pipes.JSON.inject.JsonType;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author dquest
 */
public class SQLParser {
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
    
    public boolean isFieldLine(String line){
        if(line.matches(".*`.*`.*NOT NULL.*")) return true;
        if(line.matches(".*`.*`.*DEFAULT NULL.*")) return true;
        if(line.matches(".*`.*`.*default NULL.*")) return true;
        if(line.matches(".*`.*`.*blob,.*")) return true;
        else return false;
    }
    
    public String getField4Line(String line){
        String[] split = line.split("`");
        if(split.length != 3){
            return null;
        }else{
            return split[1];
        }
    }
    
    public int getCreateLine(List<String> lines){
        int n = 0;
        for(int i=0; i<lines.size(); i++ ){
            if(lines.get(i).contains("CREATE TABLE")){
                return i;
            }
        }
        return n;
    }
    
    public int getCloseLine(List<String> lines){
        int n = 0;
        for(int i=0; i<lines.size(); i++ ){
            if(lines.get(i).matches(".*\\) ENGINE.MyISAM .*DEFAULT CHARSET.*")){
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
    
}
