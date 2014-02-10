package edu.mayo.bior.publishers.BGI;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.bior.publishers.BGIDanish.BGIPublisher;
import edu.mayo.pipes.WritePipe;
import edu.mayo.pipes.JSON.tabix.SameVariantPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.UNIX.GrepEPipe;
import edu.mayo.pipes.bioinformatics.VCF2VariantPipe;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.history.HistoryOutPipe;
import edu.mayo.bior.utils.CatalogUtils;
import edu.mayo.pipes.PrintPipe;

public class BGIPublisherTest {
	
	private final String TEST_DATA_DIR = "src/test/resources/testData/bgi/";
	
	@Test
	/** Test out a chromosome that is non-numeric - such as chrY */
	public void nonNumericChrom() throws IOException {
		final String BGI_CHRY_INPUT_TSV  = TEST_DATA_DIR + "bgi.chrY.input.tsv";
		final String BGI_CHRY_EXPECTED_TSV=TEST_DATA_DIR + "bgi.chrY.catalog.expected.tsv";
		final String BGI_CHRY_OUTPUT_TSV = TEST_DATA_DIR + "tmpOut/bgi.chrY.catalog.output.tsv";

		// Create the output catalog for chrY from input text file
		new BGIPublisher().publish(BGI_CHRY_INPUT_TSV, BGI_CHRY_OUTPUT_TSV);

		CatalogUtils.assertFileEquals(BGI_CHRY_EXPECTED_TSV, BGI_CHRY_OUTPUT_TSV);
	}
	
	@Test 
	public void chr17TenLinesBuild() throws IOException {
		// Build the catalog
		final String BGI_CHR17_INPUT_TSV    = TEST_DATA_DIR + "bgi.chr17.input.tsv";
		final String BGI_CHR17_EXPECTED_TSV = TEST_DATA_DIR + "bgi.chr17.catalog.expected.tsv";
		final String BGI_CHR17_OUTPUT_TSV   = TEST_DATA_DIR + "tmpOut/bgi.chr17.catalog.output.tsv";
		// Create the output catalog for chr17 from input text file
		new BGIPublisher().publish(BGI_CHR17_INPUT_TSV, BGI_CHR17_OUTPUT_TSV);
		CatalogUtils.assertFileEquals(BGI_CHR17_EXPECTED_TSV, BGI_CHR17_OUTPUT_TSV);
	}
	
	@Test
	public void sameVariantAgainstDbSnpBrca1() throws IOException {
		// Test against SameVariant
		final String BGI_CHR17_BGZ                 = new File(TEST_DATA_DIR + "bgi.catalog.chr17.bgz").getCanonicalPath();
		final String BGI_CHR17_SAME_VAR_EXPECTED_TSV = TEST_DATA_DIR + "bgi.chr17.catalog.sameVariant.expected.tsv";
		final String BGI_CHR17_SAME_VAR_OUTPUT_TSV   = TEST_DATA_DIR + "tmpOut/bgi.chr17.catalog.sameVariant.output.tsv";
		final String DBSNP_IN = TEST_DATA_DIR + "dbSNPS_overlap_BRCA1.vcf";
		
		System.out.println("exists?: " + new File(BGI_CHR17_BGZ).exists());
		System.out.println("dbsnp path: " + BGI_CHR17_BGZ);
		
		// Run SameVariant pipe against DbSNP variants within BRCA1 (on chr17)
		Pipeline pipe = new Pipeline(
				new CatPipe(),
				new HistoryInPipe(),
				new VCF2VariantPipe(),
				new SameVariantPipe(BGI_CHR17_BGZ),
				new HistoryOutPipe(),
				new GrepEPipe("^#"),  // Remove headers
				new GrepEPipe("\\{\\}"), // Remove any lines with a blank json object for the SameVariantPipe output
				new WritePipe(BGI_CHR17_SAME_VAR_OUTPUT_TSV, false, true)
				//new PrintPipe()
				);
        pipe.setStarts(Arrays.asList(DBSNP_IN));
		while(pipe.hasNext()) {
			pipe.next();
		}
		CatalogUtils.assertFileEquals(BGI_CHR17_SAME_VAR_EXPECTED_TSV, BGI_CHR17_SAME_VAR_OUTPUT_TSV);
	}
	
	/** Test out the String.matches() method that is the basis for GrepPipe and GrepEPipe
	 *  GrepPipe and GrepEPipe should use a "reluctant" operation in that they should NOT 
	 *  force a match of the entire string */ 
	@Test
	public void stringMatches() {
		String str = "17	63683	63683	{\"chromosomeID\":\"chr17\",\"genomic_position\":\"63683\",\"index_of_major_allele\":2,\"index_of_minor_allele\":0,\"number_A\":72,\"number_C\":0,\"number_G\":668,\"number_T\":1,\"estimatedMAF\":\"0.093004\",\"_type\":\"variant\",\"_landmark\":\"17\",\"_refAllele\":\"G\",\"_altAlleles\":[\"A\"],\"_minBP\":63683,\"_maxBP\":63683,\"_id\":\".\"}	{}";
		assertTrue(str.matches(".*?17.*?"));
		assertTrue(str.matches(".*?\\{\\}.*?"));
		assertTrue(str.matches(".*?^17.*?"));
		assertFalse(str.matches(".*?chr17a.*?"));
		assertFalse(str.matches(".*?something.*?"));
	}
	
}
