package edu.mayo.publishers;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.lf5.util.Resource;
import org.junit.Test;

public class BuildCatalog {

	String dataPath   = "src/test/resources/catalogData";
	String tempPath   = "src/test/resources/catalogTemp";
	String outputPath = "src/test/resources/catalogOut";

	// Files
	File ncbiGenesRawTarGz;
	File ncbiGenesBiorTarGz;
	File tabixIndex;
	File dbSnpChr17Vars;
	
	// Directories
	File dataDir;
	File tmpDir;
	File catalogOutDir;
	
	@Test
	public void createAndTestCatalog() throws IOException {
		initEnv();
		initFiles();
		
		verifyDataFilesExist();
		
		cleanDirs();

		buildCatalog();
		
		testOverlapPipes();
	}
	
	private void initFiles() {
		// File we will build the catalog from:
		ncbiGenesRawTarGz = new File(dataPath + "/ncbiGenes.full.tar.gz");

		// Our catalog file
		ncbiGenesBiorTarGz = new File(outputPath + "/genes.tsv.bgz");

		// Tabix index
		tabixIndex = new File(outputPath + "/genes.tsv.bgz.tbi");

		// Test variants to annotate against finished catalog:
		dbSnpChr17Vars = new File(dataPath + "/dbSNPchr17.100k.vcf.gz");

		// Directories
		dataDir = new File(dataPath);
		tmpDir  = new File(tempPath);
		catalogOutDir = new File(outputPath);
	}
	
	private void initEnv() {
		String currentDir = new File(".").getAbsolutePath();
		String userDir = System.getProperty("user.dir");
		
		System.getenv().put("BASEDIR", currentDir);
		// Dynamically grab the unzipped distribution folder name
		String folder = execCmd("ls -F target | grep \"bior.*/\" | cut -d \"/\" -f 1");
		System.getenv().put("FOLDER", folder);
	    		# setup env vars
	    		export BIOR_CATALOG_HOME=$BASEDIR/target/$FOLDER
	    		export PATH=$BIOR_CATALOG_HOME/bin:$PATH
	}
	
	private void verifyDataFilesExist() {
		// Verify original data files exist
		assertTrue(ncbiGenesRawTarGz.exists());
		assertTrue(dbSnpChr17Vars.exists());

	}

	private void cleanDirs() throws IOException {
		// Remove all temp output files
		assertTrue(tmpDir.getCanonicalPath().contains(tempPath));
		for(File f : tmpDir.listFiles()) {
			f.delete();
		}
		
		// Remove all catalog output files
		assertTrue(catalogOutDir.getCanonicalPath().contains(outputPath));
		for(File f : catalogOutDir.listFiles()) {
			f.delete();
		}
		
		// Verify that none of the output files exist initially
		assertTrue(tmpDir.listFiles().length == 0);
		assertTrue(catalogOutDir.listFiles().length == 0);
	
	}

	private void buildCatalog() throws IOException {
		// Unzip the ncbi genes raw tar gzip file to the tempDir
		System.out.println("Untar raw file...");
		String rsp = execCmd("tar -zxvf " + ncbiGenesRawTarGz.getCanonicalPath()
				+ " -C " + tmpDir.getCanonicalPath());
		System.out.println("Rsp: " + rsp);
		System.out.println("Files:");
		for(File f : tmpDir.listFiles()) {
			System.out.println("    " + f.getCanonicalPath());
		}
		
		// Build the catalog using the shell scripts
		System.out.println("Creating the catalog...");
		rsp = execCmd("source setEnv.sh");
		System.out.println("Rsp source: " + rsp);
		rsp = execCmd("cd publishers");
		rsp = execCmd("pwd");
		System.out.println("Rsp pwd: " + rsp);
		rsp = execCmd("echo $BIOR_CATALOG_HOME");
		System.out.println("Rsp $BIOR_CATALOG_HOME = " + rsp);
		rsp = execCmd("publish_NcbiGene.sh " + tempPath + " " + outputPath);
		System.out.println("Rsp: " + rsp);
		
		// Verify all output files exist and sizes and content correct
		// TODO:........
		assertTrue(ncbiGenesBiorTarGz.exists());
		assertTrue(tabixIndex.exists());

	}

	private void testOverlapPipes() throws IOException {
		// Run a command to pipe multiple commands into each other - add annotation for gene name onto end of variants input file
		System.out.println("executed large piping command...");
		String cmdOverlapVariants = "zcat " + dbSnpChr17Vars.getCanonicalPath() 
				+ " | bior_vcf_to_variants.sh | bior_overlap.sh -d " + ncbiGenesBiorTarGz.getCanonicalPath()
				+ " | grep -v {} | bior_drill.sh -p gene";
		String rsp = execCmd(cmdOverlapVariants);
		System.out.println("Response:");
		System.out.println(rsp);
		
		// Verify the size of annotations and that it just added only the one column on end
		// TODO:.........
	}

	private String execCmd(String cmd) throws IOException {
		//On windows, run: "cmd /C dir"
		Process p = Runtime.getRuntime().exec(cmd);  
        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));  
        String line = null;
        StringBuffer outBuff = new StringBuffer();
        while ((line = in.readLine()) != null) {  
            outBuff.append(line + "\n");  
        }
        return outBuff.toString();
	}
	
}
