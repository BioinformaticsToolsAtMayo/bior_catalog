package edu.mayo.bior.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import edu.mayo.bior.utils.OneBasedCatalogValidator;


public class OneBasedCatalogValidatorTest {

	@Test
	public void validateChr17() throws Exception {
		final String catalogPath 		= "src/test/resources/testData/bgi/bgi.catalog.chr17.bgz";
		final String refAlleleFastaPath = "src/test/resources/testData/util/ncbiRefs.chr17.tsv.bgz";
		
		OneBasedCatalogValidator validator = new OneBasedCatalogValidator();
		assertEquals(16, validator.verifyOneBased(catalogPath, refAlleleFastaPath, false, null));
		assertEquals(7078, validator.mTotalLines);
		assertEquals(0, validator.mNotFound);
		assertEquals(0, validator.mUnknownRef);
		assertEquals(16, validator.mMismatches);
	}

	
	
	@Test
	public void validateChr17UsingChromParam() throws Exception {
		final String catalogPath 		= "src/test/resources/testData/bgi/bgi.catalog.chr17.bgz";
		final String refAlleleFastaPath = "src/test/resources/testData/util/ncbiRefs.chr17.tsv.bgz";
		
		OneBasedCatalogValidator validator = new OneBasedCatalogValidator();
		assertEquals(16, validator.verifyOneBased(catalogPath, refAlleleFastaPath, false, "17"));
		assertEquals(7078, validator.mTotalLines);
		assertEquals(0, validator.mNotFound);
		assertEquals(0, validator.mUnknownRef);
		assertEquals(16, validator.mMismatches);
	}
	
	@Test
	public void validateChrM_noChrMInNcbiRefs() throws Exception {
		final String catalogPath 		= "src/test/resources/testData/bgi/bgi.chrM.tsv.bgz";
		final String refAlleleFastaPath = "src/test/resources/testData/util/ncbiRefs.chr17.tsv.bgz";
		
		OneBasedCatalogValidator validator = new OneBasedCatalogValidator();
		assertEquals(650, validator.verifyOneBased(catalogPath, refAlleleFastaPath));
		assertEquals(650, validator.mTotalLines);
		assertEquals(650, validator.mNotFound);
		assertEquals(0, validator.mUnknownRef);
		assertEquals(0, validator.mMismatches);
	}

}
