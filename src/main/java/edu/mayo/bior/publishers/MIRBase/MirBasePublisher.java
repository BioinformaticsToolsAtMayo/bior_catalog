/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.MIRBase;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.JSON.InjectIntoJsonPipe;
import edu.mayo.pipes.JSON.inject.ColumnInjector;
import edu.mayo.pipes.JSON.inject.Injector;
import edu.mayo.pipes.JSON.inject.JsonType;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.ReplaceAllPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.UNIX.GrepEPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.history.HCutPipe;
import edu.mayo.pipes.history.HistoryInPipe;
import java.util.Arrays;

/**
 *
 * @author m102417
 */
public class MirBasePublisher {
    
    public static void usage(){
        System.out.println("usage: MirBasePublisher <rawDataFile> <rawOutputFile>");
    }
    
    private static String infile = "src/test/resources/testData/mirbase/hsa.gff2";
    
    public static void main(String[] args){
        MirBasePublisher mbp = new MirBasePublisher();
        mbp.publish(infile,new PrintPipe());
    }
    
    public void publish(String mirbaseFile, Pipe outPipe){
        int col = 0;
        InjectIntoJsonPipe inject = new InjectIntoJsonPipe(true, new Injector[] {
            new ColumnInjector(++col, CoreAttributes._landmark.toString(), JsonType.STRING),
            new ColumnInjector(++col, "source", JsonType.STRING),    
            new ColumnInjector(++col, "type", JsonType.STRING),     
            new ColumnInjector(++col, CoreAttributes._minBP.toString(), JsonType.STRING),
            new ColumnInjector(++col, CoreAttributes._maxBP.toString(), JsonType.STRING),
            new ColumnInjector(++col, "score", JsonType.STRING),
            new ColumnInjector(++col, CoreAttributes._strand.toString(), JsonType.STRING),
            new ColumnInjector(++col, "phase", JsonType.STRING),  
            new ColumnInjector(++col+1, "ACC", JsonType.STRING),  
            new ColumnInjector(++col+2, "ID", JsonType.STRING)     
                
        });
        int[] cut = {2,3,6,7,8,9,10,11,12,13};
        Pipeline p = new Pipeline(
                new CatPipe(),
                new GrepEPipe(".*#"),
                new ReplaceAllPipe("\"","\t"),
                new ReplaceAllPipe("^chr",""),
                new HistoryInPipe(),
                inject,
                new HCutPipe(cut),
                outPipe
                );
        p.setStarts(Arrays.asList(infile));
        for(int i=0; p.hasNext(); i++){
            p.next();
        }
    }
}
