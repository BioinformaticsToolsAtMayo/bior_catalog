package edu.mayo.bior.publishers.HapMap;

import com.tinkerpop.pipes.AbstractPipe;
import edu.mayo.pipes.JSON.tabix.SameVariantLogic;
import edu.mayo.pipes.history.History;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Given a list of variants as JSON in the last column, this pipe collapses them into a single 
 * variant IF they are the same (as compared by SameVariant).
 * Ex (3 variants in, 2 output (1 merge - A & B are same) )):
 * Inputs: 
 * 		A:	{"_landmark":"1","_rsId":"rs123","_minBP":100,"population":"CHN","QcCode":"QC+","refAllele":"T"}
 * 		B:	{"_landmark":"1","_rsId":"rs123","_minBP":100,"population":"JPN","QcCode":"QC-","refAllele":"G"}
 * 		C:	{"_landmark":"2","_rsId":"rs210","_minBP":200,"population":"EUR","QcCode":"x","refAllele":"C"}
 * Outputs: 
 * 		A:	{"_landmark":"1","_rsId":"rs123","_minBP":100,"CHN":{"QcCode":"QC+","refAllele":"T"},"JPN":{"QcCode":"QC-","refAllele":"G"}}
 * 		C:	{"_landmark":"2","_rsId":"rs210","_minBP":200,"population":"EUR","QcCode":"x","refAllele":"C"}
 * @author m102417 - Daniel Quest, Michael Meiners
 */
public class CollapseHapMapVariantsPipe extends AbstractPipe<History,History> {
    SameVariantLogic m_sameVariantLogic = new SameVariantLogic();;
    HapMapQueue m_queueUtil = new HapMapQueue();

    // The next line to output - hold data until we can verify whether the next line is the same variant to merge with.
    private  History m_queue = null;
    
    public CollapseHapMapVariantsPipe() throws IOException{
    }

    @Override
    //================================================================
    // Example: (where variant A1 == A2 (but whose population data are probably different), C1 == C2, and x means no data)
    //          and Queue is the data that will be output to the next pipe
    // Input	Queue (Q)	Step/Output					Comment
    // -------	-------		-------------				-----------------------------------------------
    // A1		x			Q = A
    //						return processNextStart()	Need to save A to Queue so we can compare to next line -- so get next input (do a recursive call to pull in next line)
    // A2		A1			Q = Merge A1&A2					Add A2's population to A1
    //						return processNextStart()	
    // B		A1&A2		Q = B
    //						return A1&A2
    // C1		B			Q = C1
    //						return B
    // C2		C1			Q = Merge C1&C2
    //						return processNextStart()
    // D		C1&C2		Q = D
    //						return C1&C2
    // x		D			Q = x
    //						return D
    // x 		x			throw NoSuchElementException
    //================================================================
    protected History processNextStart() throws NoSuchElementException {

    	// If the queue is empty and there is no input data, then throw exception to end pipe
        if( m_queue == null  && ! starts.hasNext() )
        	throw new NoSuchElementException("No more data - ending pipe...");
        // Else, if there is no more input, then just return the queue (but empty the queue so next time it will be empty and we will end the loop)
        else if( ! starts.hasNext() ) {
        	History temp = m_queue;
        	m_queue = null;
        	return temp;
        }

        History input  = (History)this.starts.next();
    	History output = (History)(input.clone());

    	tierJsonColumn(output);

    	// If the queue is empty, then this is our first line of data
        if( m_queue == null ) {
        	m_queue = output;
        	return processNextStart();
        } else if( isSameVariant(output, m_queue) ) {
        	m_queue = merge(m_queue, output);
        	return processNextStart();
        } else {  // Variants are different, so can return the one in the queue
        	History temp = m_queue;
        	m_queue = output;
        	return temp;
        }
    }


    /** Tier the JSON column (modify the last column in the History, which is the flat JSON string,
     *  and separate the population-specific elements into their own sub-element). 
     *  Ex:
     *  	"A":1,"B":2,"population":"CHN","QcCode":"QC+","refAllele":"T"    becomes:
     *   	"A":1,"B":2,"CHN":{"QcCode":"QC+","refAllele":"T"}    */
	private void tierJsonColumn(History output) {
        String tieredJsonVariant = m_queueUtil.collapsePopulation(getJson(output));
        output.set(output.size()-1, tieredJsonVariant);
	}

    /** Given JSON within the last column, push population info up into a sub-JSON element within the JSON string
     *  Ex: Before: "population":"CHN","center":"perlegen","QC_code":"QC+",...
     *       After: "CHN":{"center":"perlegen","QC_code":"QC+",...}
     */
	private History merge(History hist1, History hist2) {
        String jsonMerged = m_queueUtil.mergeHapMap(getJson(hist1), getJson(hist2));
        History newHist = (History)(hist1.clone());
        newHist.set(newHist.size()-1, jsonMerged);
        return newHist;
	}

	private boolean isSameVariant(History variantHistory1, History variantHistory2) {
		return m_sameVariantLogic.same(getJson(variantHistory1), getJson(variantHistory2));
	}
	
	private String getJson(History history) {
		return history.get(history.size()-1);
	}
}
