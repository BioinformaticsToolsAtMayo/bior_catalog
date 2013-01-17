/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.HapMap;

import java.io.IOException;
import java.util.Arrays;

import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.JSON.DrillPipe;
import edu.mayo.pipes.JSON.InjectIntoJsonPipe;
import edu.mayo.pipes.JSON.inject.ColumnArrayInjector;
import edu.mayo.pipes.JSON.inject.ColumnInjector;
import edu.mayo.pipes.JSON.inject.Injector;
import edu.mayo.pipes.JSON.inject.JsonType;
import edu.mayo.pipes.JSON.inject.LiteralInjector;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.bioinformatics.vocab.Type;
import edu.mayo.pipes.history.FindAndReplaceHPipe;
import edu.mayo.pipes.history.HCutPipe;
import edu.mayo.pipes.history.HistoryInPipe;

/**
 *
 * @author m102417
 */
public class HapMapPublisherPhase2 {
    public static void usage(){
        System.out.println("usage: HapMapPublisherPhase2 <rawDataDir> <catalogOutputDir>");
    }
    
    public static void main(String[] args) throws IOException {	 
        String infile = "hapmap.sorted.liftover.tsv";
        HapMapPublisherPhase2 publisher = new HapMapPublisherPhase2();
        publisher.publish("/data/catalogs/hapmap/2010-08_phaseII+III"+"/scratch", infile);
//        System.out.println(args.length);
//        if(args.length >= 1){ 
//            publisher.publish(args[1], args[2] + "/scratch/" + infile);
//        }else{
//            usage();
//            System.exit(1);
//        }
    } 
    
    public void publish(String scratchDir, String infile) throws IOException {
        String[] paths = new String[3];
        paths[0] = "refallele";
        paths[1] = "otherallele";
        paths[2] = "rsNumber";
        int[] col = new int[] {2,3,6,8};
        DrillPipe drill = new DrillPipe(true, paths);
        FindAndReplaceHPipe replaceChr = new FindAndReplaceHPipe(1, "chr", "");
        Injector[] injectors = new Injector[]
        		{
        			new LiteralInjector(CoreAttributes._type.toString(), Type.VARIANT.toString(), JsonType.STRING),
        			new ColumnInjector     (1, CoreAttributes._landmark.toString(),   JsonType.STRING),
        			new ColumnInjector     (2, CoreAttributes._minBP.toString(),      JsonType.NUMBER),
        			new ColumnInjector     (3, CoreAttributes._maxBP.toString(),      JsonType.NUMBER),
        			new ColumnInjector     (4, CoreAttributes._strand.toString(),     JsonType.STRING),
        			new ColumnInjector     (5, CoreAttributes._refAllele.toString(),  JsonType.STRING),
        			new ColumnArrayInjector(6, CoreAttributes._altAlleles.toString(), JsonType.STRING, ","),
        			new ColumnInjector     (7, CoreAttributes._id.toString(),         JsonType.STRING)        			        			        			
        		};
        
        InjectIntoJsonPipe inject = new InjectIntoJsonPipe(8, injectors);                    
        int[] cut2 = new int[] {1,2,3,4,5,6,7};
        Pipeline p = new Pipeline(new CatPipe(), 
                                  new HistoryInPipe(), 
                                  new HCutPipe(col),
                                  drill,                //construct the golden attributes
                                  replaceChr,
                                  inject,               //inject the golden attributes into the json
                                  new HCutPipe(cut2),
                                  //new CollapseHapMapVariantsPipe(), 
                                  new PrintPipe());
        p.setStarts(Arrays.asList(scratchDir+"/"+infile));
        for(int i=0; p.hasNext(); i++){
            p.next();
            if(i>100) break;
        }
    }
}
