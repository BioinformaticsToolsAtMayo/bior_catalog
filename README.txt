
= Building =

	You can build the project by running using the following different methods:
	
	1. COMMAND LINE:	mvn clean package
	
	2. M2ECLIPSE:		Right-click and select Run As --> Maven Build...
						Under Goals enter "clean package"
						Click Run
	
	The maven build produces the build artifacts under the ${project}/target folder.

	
= Setup =
	
	Prior to running any project executables, the following must be setup:
		
	1. Setup your UNIX environment:  (this should be done for both the bior_catalog and bior_pipeline projects)

		1.1		open terminal
		1.2		cd ${project}
		1.3		mvn clean package	# runs the build to create the distribution
		1.4		source setupEnv.sh	# inspects the target folder to setup env vars	
	
	2. Tabix application
			
			Download tabix-0.2.5.tar.bz2 from  http://sourceforge.net/projects/samtools/files/tabix
			unzip tabix-0.2.5.tar.bz2 to your local harddrive (e.g. /Applications/tabix-0.2.5)
			cd /Applications/tabix-0.2.5
			make
			add the following to your shell profile: export PATH=/Applications/tabix-0.2.5:$PATH
	
= Running =
    To build the catalog, do this (from project root):
        cd publishers
        ./publish_NcbiGene.sh  <pathToRawDataDir>  <pathToPublishOutputDir>
    For example:
        ./publish_NcbiGene.sh  /data/catalogs/NCBIGene/GRCh37_p10/scratch/  /data/catalogs/NCBIGene/GRCh37_p10/
	
	Now, after having sourced the setupEnv.sh file, you should be able to run these commands
	   1) Dump the contents of a small vcf file
    	    zcat /data/dbsnp/dbsnp20k.vcf.gz 
	   2) Now build this into a variant object
	          