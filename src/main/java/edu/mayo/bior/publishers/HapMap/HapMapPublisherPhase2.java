package edu.mayo.bior.publishers.HapMap;

import java.io.IOException;
import java.util.Arrays;

import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.WritePipe;
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
import edu.mayo.pipes.history.HistoryOutPipe;

/**
 * @author m102417 (Daniel Quest), Michael Meiners
 */
public class HapMapPublisherPhase2 {
    public static void usage(){
        System.out.println("usage: HapMapPublisherPhase2 <HapmapSortedLiftoverTsvFile> <hapmapCollapsedJsonFile>");
    }
    
    public static void main(String[] args) throws IOException {
    	if(args.length != 2) {
    		usage();
    		throw new IllegalArgumentException("Arguments incorrect - see usage");
    	}
        String inFile = args[0];
        String outFile = args[1];
        HapMapPublisherPhase2 publisher = new HapMapPublisherPhase2();
        publisher.publish(inFile, outFile);
    } 
    
    /** Given the Hapmap liftover tsv file, produce a new text file that has 
     *  the variants' populations condensed to one line per same variant (as a JSON object)
     * @param inFile  The sorted results from the Hapmap liftover (in tab-delimited file)
     * @param outFile The file to dump the variants whose populations have been condensed to one line per same variant
     * @throws IOException
     */
    public void publish(String inFile, String outFile) throws IOException {
        DrillPipe drill = new DrillPipe(true, new String[] { "refallele", "otherallele", "rsNumber" });
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
        Pipeline p = new Pipeline(
        	new CatPipe(), 
        	new HistoryInPipe(),
        	// Cut out empty columns
        	new HCutPipe(new int[] {2,3,6,8}),
        	drill,                //construct the golden attributes
        	replaceChr,
        	inject,               //inject the golden attributes into the json
        	// Cut out all columns except chrom, start, end, json
        	new HCutPipe(new int[] {4,5,6,7}),
        	new CollapseHapMapVariantsPipe(),
        	new MergePipe("\t", false),
        	new WritePipe(outFile, false, true)
        	//new PrintPipe()
        	);
        p.setStarts(Arrays.asList(inFile));
        while(p.hasNext()){
            p.next();
        }
    }
}
