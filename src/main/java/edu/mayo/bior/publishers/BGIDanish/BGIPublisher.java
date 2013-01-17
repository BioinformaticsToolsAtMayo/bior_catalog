/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.BGIDanish;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.transform.TransformFunctionPipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.bior.publishers.Cosmic.CosmicPublisher;
import edu.mayo.pipes.HeaderPipe;
import edu.mayo.pipes.JSON.InjectIntoJsonPipe;
import edu.mayo.pipes.JSON.inject.ColumnArrayInjector;
import edu.mayo.pipes.JSON.inject.ColumnInjector;
import edu.mayo.pipes.JSON.inject.Injector;
import edu.mayo.pipes.JSON.inject.JsonType;
import edu.mayo.pipes.JSON.inject.LiteralInjector;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrependStringPipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.UNIX.CatGZPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.UNIX.LSPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.bioinformatics.vocab.Type;
import edu.mayo.pipes.history.HCutPipe;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.history.HistoryOutPipe;
import edu.mayo.pipes.util.GenomicObjectUtils;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Date;

/**
 *
 * @author m102417
 */
public class BGIPublisher {
    public static void usage(){
        System.out.println("usage: BGIPublisher <rawDataFile> <catalogOutputDir>");
    }
    
    public static void main(String[] args) {	 
        BGIPublisher publisher = new BGIPublisher();
        publisher.publish("/data/BGI/hg19/LuCAMP_200exomeFinal_hg19.txt", "/tmp", new PrintPipe());
//        System.out.println(args.length);
//        if(args.length >= 1){ 
//            publisher.publish(args[1], args[2] + "/scratch/");
//        }else{
//            usage();
//            System.exit(1);
//        }
    } 
    
    public void publish(String rawDataFileFullpath, String outputDir, Pipe out) {
        final String catalogFile = "LuCAMP_200exomeFinal.tsv";
        double start = System.currentTimeMillis();
        System.out.println("Started loading HapMap at: " + new Timestamp(new Date().getTime()));
        String outfile = outputDir + "/" + catalogFile;
        System.out.println("Outputing File to: " + outfile);
        Pipe<History,History> t = new TransformFunctionPipe<History,History>( new BGIPipe() );
        String[] header = new String[] {
                                        "chromosomeID",
                                        "genomic_position",
                                        "index_of_major_allele",
                                        "index_of_minor_allele",
                                        "number_A",
                                        "number_C",
                                        "number_G",
                                        "number_T",
                                        "estimatedMAF",
                                        CoreAttributes._type.toString(),
                                        CoreAttributes._landmark.toString(),
                                        //CoreAttributes._refAllele.toString(),
                                        CoreAttributes._altAlleles.toString(),
                                        CoreAttributes._minBP.toString(),
                                        CoreAttributes._maxBP.toString()
                                            };
        Injector[] injectors = new Injector[] {
            new ColumnInjector(1, "chromosomeID", JsonType.STRING),
            new ColumnInjector(2, "genomic_position", JsonType.STRING),
            new ColumnInjector(3, "index_of_major_allele", JsonType.STRING),
            new ColumnInjector(4, "index_of_minor_allele", JsonType.STRING),
            new ColumnInjector(5, "number_A", JsonType.STRING),
            new ColumnInjector(6, "number_C", JsonType.STRING), 
            new ColumnInjector(7, "number_G", JsonType.STRING),
            new ColumnInjector(8, "number_T", JsonType.STRING),
            new ColumnInjector(9, "estimatedMAF", JsonType.STRING),
            new ColumnInjector(10, CoreAttributes._type.toString(), JsonType.STRING),
            new ColumnInjector(11, CoreAttributes._landmark.toString(), JsonType.STRING),
            new ColumnInjector(12, CoreAttributes._refAllele.toString(), JsonType.STRING),
            new ColumnArrayInjector(13, CoreAttributes._altAlleles.toString(), JsonType.STRING, ","),
            new ColumnInjector(14, CoreAttributes._minBP.toString(), JsonType.STRING),
            new ColumnInjector(15, CoreAttributes._maxBP.toString(), JsonType.STRING)       			        			        			
        };
        InjectIntoJsonPipe inject = new InjectIntoJsonPipe(injectors);
        int[] cut = new int[] {1,2,3,4,5,6,7,8,9,10,12,13};
        int[] cut2 = new int[] {1};
        Pipe p = new Pipeline(new CatPipe(),
                             new HistoryInPipe(),
                             t,
                             inject,
                             //new HCutPipe(false, cut), 
                             new MergePipe("\t"),
                             out);
        p.setStarts(Arrays.asList(rawDataFileFullpath));
        for(int i=0;p.hasNext();i++){
            p.next();
            if(i>100) break;
        }
    }
    
    public class BGIPipe implements PipeFunction<History,History>{

        /*
         * col1: chromosomeID
           col2: genomic position
           col3: index_of_major_allele
           col4: index_of_minor_allele
           col5: # of A
           col6: # of C
           col7: # of G
           col8: # of T
           col9: estimatedMAF

           indexs 0, 1, 2, 3 correspond to A, C, G, T.
         */
        private String decode(String s){
            if(s.endsWith("0")){
                return "A";
            }else if(s.endsWith("1")){
                return "C";
            }else if(s.endsWith("2")){
                return "G";
            }else if(s.endsWith("3")){
                return "T";
            }
            return null;
        }

        
        @Override
        public History compute(History h) {
            //_type
            h.add(Type.VARIANT.toString());
            //_landmark
            h.add(  GenomicObjectUtils.computechr(h.get(0) ));
            //_minBP
            h.add(decode(h.get(2)));
            //_maxBP
            h.add(decode(h.get(3)));
            //some checking on hg19 shows it to be one based not zero based
            h.add((h.get(1)));
            h.add((h.get(1)));
            return h;
        }
        
    }
    
}
