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
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrependStringPipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.UNIX.CatGZPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.UNIX.LSPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.bioinformatics.vocab.Type;
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
        publisher.publish("/data/BGI/hg19/LuCAMP_200exomeFinal_hg19.txt", "/tmp");
//        System.out.println(args.length);
//        if(args.length >= 1){ 
//            publisher.publish(args[1], args[2] + "/scratch/");
//        }else{
//            usage();
//            System.exit(1);
//        }
    } 
    
    public void publish(String rawDataFileFullpath, String outputDir) {
        final String catalogFile = "LuCAMP_200exomeFinal.tsv";
        double start = System.currentTimeMillis();
        System.out.println("Started loading HapMap at: " + new Timestamp(new Date().getTime()));
        String outfile = outputDir + "/" + catalogFile;
        System.out.println("Outputing File to: " + outfile);
        Pipe<History,History> t = new TransformFunctionPipe<History,History>( new BGIPipe() );
//        InjectIntoJsonPipe inject = new InjectIntoJsonPipe(14, 
////                                                      new AbstractMap.SimpleEntry(CoreAttributes._type.toString(), Type.VARIANT.toString()), 
////                                                      new AbstractMap.SimpleEntry("1",CoreAttributes._landmark.toString()), 
////                                                      new AbstractMap.SimpleEntry("2", CoreAttributes._minBP.toString()),
////                                                      new AbstractMap.SimpleEntry("3", CoreAttributes._maxBP.toString()),
////                                                      new AbstractMap.SimpleEntry("4", CoreAttributes._strand.toString()),
////                                                      new AbstractMap.SimpleEntry("5", CoreAttributes._refAllele.toString()),
//                                                      new AbstractMap.SimpleEntry("10", CoreAttributes._type.toString()),
//                                                      new AbstractMap.SimpleEntry("11", CoreAttributes._landmark.toString()),
//                                                      new AbstractMap.SimpleEntry("12", CoreAttributes._refAllele.toString()),
//                                                      new AbstractMap.SimpleEntry("13", CoreAttributes._altAlleles.toString())
//        );
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
                                        CoreAttributes._refAllele.toString(),
                                        CoreAttributes._altAlleles.toString()
                                            };
        InjectIntoJsonPipe inject = new InjectIntoJsonPipe(header);
        Pipe p = new Pipeline(new CatPipe(),
                             new HistoryInPipe(),
                             t,
                             inject,
                             new MergePipe("\t"),
                             new PrintPipe());
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
            return h;
        }
        
    }
    
}
