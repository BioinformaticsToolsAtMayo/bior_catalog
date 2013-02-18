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
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.ReplaceAllPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.UNIX.GrepEPipe;
import edu.mayo.pipes.WritePipe;
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
    
    
    
    public static void main(String[] args){
        MirBasePublisher mbp = new MirBasePublisher();
        
        if( args.length != 2 ) {
            usage();
            System.exit(1);
        }

        String infile = args[0];
        //String infile = "src/test/resources/testData/mirbase/hsa.gff2";
        String outfile = args[1];
        System.out.println("Input File:  " + infile);
        System.out.println("Output File: " + outfile);  
        //EX: mbp.publish(infile,new PrintPipe());
        // Write to file: Don't append to file;  Add newlines to each line
        Pipe out = new WritePipe(outfile, false, true);
        mbp.publish(infile,out);
        
    }
    
    public Pipeline getPipeline(Pipe outPipe){
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
                new MergePipe("\t"),
                outPipe
                );
        return p;
    }
    
    public void publish(String mirbaseFile, Pipe outPipe){
        Pipeline p = getPipeline(outPipe);
        p.setStarts(Arrays.asList(mirbaseFile));
        for(int i=0; p.hasNext(); i++){
            p.next();
        }
    }
}
