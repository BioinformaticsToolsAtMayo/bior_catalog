/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.bior.publishers.HapMap;

import com.tinkerpop.pipes.AbstractPipe;
import edu.mayo.pipes.JSON.tabix.SameVariantLogic;
import edu.mayo.pipes.JSON.tabix.SameVariantPipe;
import edu.mayo.pipes.bioinformatics.vocab.ComparableObjectInterface;
import edu.mayo.pipes.history.History;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Given a list of variants in the last column, this pipe collapses them into a single 
 * variant.
 * @author m102417
 */
public class CollapseHapMapVariantsPipe extends AbstractPipe<History,History> {
    SameVariantLogic sameVariantLogic;
    public CollapseHapMapVariantsPipe() throws IOException{
        sameVariantLogic = new SameVariantLogic();
    }
    
    

    @Override
    protected History processNextStart() throws NoSuchElementException {
        History h = this.starts.next();
        //sameVariantLogic.same("", "");        
//        String alt = h.get(9);
//        if(alt.length()>1){
//            System.out.println(alt);
//        }
        return h;
    }
    

    
}
