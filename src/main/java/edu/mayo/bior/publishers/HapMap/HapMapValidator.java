/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.HapMap;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.JSON.DrillPipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.UNIX.CatGZPipe;
import edu.mayo.pipes.bioinformatics.sequence.Bed2SequencePipe;
import edu.mayo.pipes.history.HistoryInPipe;
import java.io.IOException;
import java.util.Arrays;

/**
 *
 * @author m102417
 */
public class HapMapValidator {
    
    public static void main(String[] args) throws IOException{
        String[] drillpath = {"refallele"};
        String hapmapCatalog = "/data/catalogs/hapmap/2010-08_phaseII+III/allele_freqs.grch37.tsv.bgz";
        String genomeCatalog = "/data/catalogs/NCBIGenome/GRCh37.p10/hs_ref_genome.fa.tsv.bgz";
        Pipe p = new Pipeline(
                new CatGZPipe("gzip"),
                new HistoryInPipe(),
                new DrillPipe(false, drillpath),
                new Bed2SequencePipe(genomeCatalog, -1),
                new PrintPipe()
                );
        p.setStarts(Arrays.asList(hapmapCatalog));
        for(int i=0; p.hasNext(); i++){
            p.next();
            if(i==1000) break;
        }
        return;
    }
    
}
