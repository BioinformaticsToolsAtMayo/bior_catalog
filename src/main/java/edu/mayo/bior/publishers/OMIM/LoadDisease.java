/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.OMIM;

import java.io.File;
import java.io.IOException;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;

import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.util.SystemProperties;


/**
 *  not needed for TREAT!
 * @author m102417
 */
public class LoadDisease {
                
    private static String version = "unknown";
    
    public static void main(String[] args) throws IOException{

            SystemProperties sysprop = new SystemProperties();

            //Used only for version
            String censusFile = sysprop.get("bior.etl.data.OMIM") + File.separator + "omim.txt.Z";
            System.out.println(censusFile);
            
            LoadDisease loader = new LoadDisease();
            int batch = 1; //These records are really big, try to keep this low
            PrintPipe insert = new PrintPipe();
            //TODO: switch to the actual loader pipe when it is ready
            loader.exec(censusFile, insert);
      
    }
    
    public void exec(String filename, Pipe insert){
//        this.version = version;
//        Pipe<ArrayList,Disease> transform = new TransformFunctionPipe<ArrayList,Disease>(new LoadDisease.OMIM2Disease());
//    	Pipe pipeline = new Pipeline(new CatGZPipe("gunzip"), new TokenAggregatorPipe("RECORD"), transform, new AggregatorPipe(batch), insert);
//    	pipeline.setStarts(Arrays.asList(filename));
//    	for(int i=0;pipeline.hasNext(); i++){
//    		pipeline.next();
//                if(i==30){
//                    System.exit(0);
//                }
//    	}
    }
    
    public static class OMIM2Disease implements PipeFunction<History,History> {
        public History compute(History h) {
//            	Disease d = null;
//                int namePos = 0;
//                if (tokens.size() > 3) {
//                        Iterator it = tokens.iterator();
//                        boolean indescription = false;
//                        StringBuilder sb2 = new StringBuilder();
//                        for(int i=0; it.hasNext();i++){
//                            String line = (String) it.next();
//                            if(line.matches("\\*FIELD\\*.+TI$")){
//                                namePos = i+1;
//                            }
//                            if(indescription){                                
//                                if(line.matches("\\*FIELD\\*.+$")){
//                                    indescription = false;
//                                }else{
//                                    //System.out.println(line);
//                                    sb2.append(line + " ");
//                                }
//                            }   
// 
//                            if(line.matches("\\*FIELD\\*.+TX$")){
//                                indescription = true;
//                            }                         
//                            
//                        }
//                        
//                        StringBuilder sb = new StringBuilder();
//                        for(int i = 0; i<tokens.size();i++){
//                            sb.append(tokens.get(i));
//                        }
//                        d = new Disease();
//                        d.setVersion(version);
//                        d.setId((String) tokens.get(2));
//                        d.setNspace(NSpace.OMIM);
//                        String name = (String) tokens.get(namePos);
//                        //remove the identifier from the name
//                        name = name.substring(7);
//                        d.setName(name.trim());
//                        d.setDescription(sb2.toString());
//                        //System.out.println(d.toString()); 				
//                }
		return h;
        }   
    } 
    
    
}
