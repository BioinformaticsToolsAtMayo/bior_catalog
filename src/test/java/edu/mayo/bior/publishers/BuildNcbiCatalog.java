package edu.mayo.bior.publishers;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.log4j.lf5.util.Resource;
import org.junit.Test;

public class BuildNcbiCatalog {

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
	public void createAndTestCatalog() throws IOException, InterruptedException {
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
	
	private void initEnv() throws IOException {
		String currentDir = new File(".").getCanonicalPath();
		String userDir = System.getProperty("user.dir");
		
		String x1 = execCmd("sh export BASEDIR=" + currentDir);
		// Dynamically grab the unzipped distribution folder name
		String folder = execCmd("ls -F target | grep \"bior.*/\" | cut -d \"/\" -f 1");
		String x2 = execCmd("sh export FOLDER=" + folder);
		System.out.println("x1 = " + x1);
		System.out.println("x2 = " + x2);
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

	private void buildCatalog() throws IOException, InterruptedException {
		
		ArrayList<String> output = execCmds(new String[] { "cd publishers", "ls -la" });
		System.out.println(output);
		
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
		rsp = execCmd("sh source setEnv.sh");
		System.out.println("Rsp source: " + rsp);
		rsp = execCmd("cd publishers");
		rsp = execCmd("pwd");
		System.out.println("Rsp pwd: " + rsp);
		rsp = execCmd("echo $BIOR_CATALOG_HOME");
		System.out.println("Rsp $BIOR_CATALOG_HOME = " + rsp);
		rsp = execCmd("sh publish_NcbiGene.sh " + tempPath + " " + outputPath);
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
	
	private ArrayList<String> execCmds(String[] cmds) throws IOException, InterruptedException {
		Process proc = Runtime.getRuntime().exec("pwd");
		// Empty the output buffer
		BufferedInputStream resultsInfo = new BufferedInputStream(proc.getInputStream());
		BufferedInputStream resultsError= new BufferedInputStream(proc.getErrorStream());
		String junk = readInStream(resultsInfo);
		String junk2= readInStream(resultsError);
		PrintWriter cmdsIn = new PrintWriter(proc.getOutputStream());
		ArrayList<String> results = new ArrayList<String>();
		for(String cmd : cmds) {
			cmdsIn.println("sh " + cmd);
			cmdsIn.flush();
			proc.waitFor();
			String info = readInStream(resultsInfo);
			String error = readInStream(resultsError);
			results.add(info + (error.length() > 0 ? "\n" : "") );
		}
		cmdsIn.close();
		int returnCode = proc.waitFor();
		System.out.println("Return code = " + returnCode);
		return results;
	}
	
	private String readInStream(InputStream in) throws IOException {
		byte[] buf = new byte[32*1024];
		int len = -1;
		String s = "";
		while( (len = in.read(buf)) != -1 )
			s += new String(buf, 0, len);
		return s;
	}
}
