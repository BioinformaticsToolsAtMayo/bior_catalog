/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.UCSC;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.transform.IdentityPipe;
import com.tinkerpop.pipes.transform.TransformFunctionPipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.bior.publishers.BGIDanish.BGIPublisher;
import edu.mayo.pipes.JSON.DrillPipe;
import edu.mayo.pipes.JSON.InjectIntoJsonPipe;
import edu.mayo.pipes.JSON.inject.ColumnInjector;
import edu.mayo.pipes.JSON.inject.Injector;
import edu.mayo.pipes.JSON.inject.JsonType;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.ReplaceAllPipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.UNIX.CatGZPipe;
import edu.mayo.pipes.UNIX.GrepPipe;
import edu.mayo.pipes.UNIX.LSPipe;
import edu.mayo.pipes.WritePipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.bioinformatics.vocab.Type;
import edu.mayo.pipes.bioinformatics.vocab.Undefined;
import edu.mayo.pipes.history.HCutPipe;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.util.GenomicObjectUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author m102417
 */
public class UCSCPublisher {
    
    public static void usage(){
        System.out.println("usage: UCSCPublisher <rawDataDir> <rawOutputDir>");
    }
    
    public static void main(String[] args) throws IOException {	 
        UCSCPublisher publisher = new UCSCPublisher();
        
        if( args.length != 2 ) {
            usage();
            System.exit(1);
        }

        //String[] dirs = {"src/test/resources/testData/ucsc/","/tmp/"};
        //String[] dirs = {"/data/ucsc/hg19/","/tmp/test"};
        //args = dirs;
        String indir = args[0];
        String outdir = args[1];
        System.out.println("Input Directory:  " + indir);
        System.out.println("Output Directory: " + outdir); 
        publisher.publish(indir, outdir, true);//use true for reporting if you want a report, and uncomment the break below so it just ouputs a few lines in the process method
    } 
    
    public int[] cutArr(int size){
        int[] c = new int[size];
        for(int i=0;i<size;i++){
            c[i]=i+1;
            //System.out.println(c[i]);
        }
        return c;
    }
    
    public void publish(String indir, String outdir, boolean reporting) throws IOException{
        List<String> sqls = getSQL(indir);
        Pipeline p = new Pipeline(
                new ReplaceAllPipe("sql$", "txt.gz")
                );
        p.setStarts(sqls);
        for(int i=0; p.hasNext(); i++){
            String file = (String) p.next();
            publishOne(indir, file, outdir, sqls.get(i), reporting);
            
        }     
    }
    
    //coriellDelDup.txt.gz
    
    public void publishOne(String indir, String file, String outdir, String sql, boolean reporting) throws IOException{
            SQLParser sqlp = new SQLParser();        
            System.out.println("Parsing SQL: " + indir + sql);
            List<String> lines = sqlp.loadFileToMemory(indir + sql);
            System.out.println("Processing: " + indir + file);
            Injector[] inj = sqlp.getInjectorsFromSQL(lines);
            //Inject the original data into the JSON
            InjectIntoJsonPipe inject = new InjectIntoJsonPipe(true, inj);
            //cut out the original data
            HCutPipe cut = new HCutPipe(false, cutArr(inj.length));
            //TODO: need to drill out the golden attrs
            DrillPipe drill = new DrillPipe(true, sqlp.getGoldenDrillPaths(lines, reporting));
            //write the output file
            String outfile = file.replaceAll("txt.gz$", "tsv");
            WritePipe write = new WritePipe(outdir + "/" + outfile, false, true);
            process(indir + file, inject, cut, drill, write); 
            write.close();
    }
    
    // the reason all these pipes get passed to this method is that they need to be initialized based on the SQL processing code.
    public void process(String datafile, Pipe inject, Pipe cut, Pipe drill, Pipe write){
        //need to compute the corect values for the golden attrs
        Pipe<History,History> xformPipe = new TransformFunctionPipe<History,History>( new UCSCCordFix() );
        // inject the goldens back in
        InjectIntoJsonPipe goldject = new InjectIntoJsonPipe(false, new Injector[] {
            new ColumnInjector(1, CoreAttributes._landmark.toString(), JsonType.STRING),
            new ColumnInjector(2, CoreAttributes._minBP.toString(), JsonType.NUMBER),
            new ColumnInjector(3, CoreAttributes._maxBP.toString(), JsonType.NUMBER)
        });
        Pipeline p = new Pipeline(
                new CatGZPipe("gzip"),
                new HistoryInPipe(),
                inject,
                cut,
                drill,
                xformPipe,
                goldject,
                new MergePipe("\t"),
                //new PrintPipe()
                write
                );
        p.setStarts(Arrays.asList(datafile));
        for(int i=0; p.hasNext(); i++){
            p.next();
            //if(i==200) break; //temporary!
        }
    }
    
    public List<String> getSQL(String indir){
        ArrayList<String> sqls = new ArrayList<String>();
        Pipeline p = new Pipeline(
                new LSPipe(false), 
                new GrepPipe(".*sql")//,
                //new PrintPipe()
                );
        p.setStarts(Arrays.asList(indir));
        for(int i=0;p.hasNext(); i++){
            String n = (String) p.next();
            //banned list: (don't try to push data for these
            if(n.contains("tableDescriptions")){
                //do nothing... This is where the metadata is!
            }else if(n.contains("trackDb")){
                //don't care about all the tracks
            }else if(n.contains("hgFindSpec")){
                //don't care about all the tracks
            }else if(n.contains("history")){
                //don't care about all the tracks
            }else{
                sqls.add(n);
            }
        }
        return sqls;
    }
    
    /**
     * Fix the UCSC funky cordinate system to be the standard cordinate system:
     * 1-based fully closed.  for more info go here:
     * http://genomewiki.ucsc.edu/index.php/Coordinate_Transforms
     * 
     */
    public class UCSCCordFix implements PipeFunction<History,History>{

        @Override
        public History compute(History a) {
            if(a.size() > 4){
                //System.out.println("Size is greater than 4");
                //clean this stuff up
                a.remove(0);
            }
            String chr = GenomicObjectUtils.computechr(a.remove(0));
            a.add(0, chr);
            if(Undefined.UNKNOWN.toString().equalsIgnoreCase(chr)){
                a.remove(1);
                a.add(1, "0");
                a.remove(2);
                a.add(2, "0");
            }else {
                if(a.size() != 4){
                    System.out.println("The following input is not of length 4:");
                    System.out.println(a.toString());
                    System.exit(0);
                }
                //do the math to convert from ucsc cords to one-based fully closed.
                String start = a.remove(1);
                String end = a.remove(1);
                if(start.equalsIgnoreCase(".")){
                    start = "0";
                }
                if(end.equalsIgnoreCase(".")){
                    end = "0";
                }
                
                Integer s = new Integer(start);
                Integer e = new Integer(end);
                if(s > e){
                    System.out.println("This file contained minBP > maxBP, that is illigal!");
                    System.exit(0);
                }
                s = s+1;
                a.add(1, e.toString());               
                a.add(1, s.toString());
            }
            return a;
        }
        
        
    
    }
}
