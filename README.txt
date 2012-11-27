
= Building =

	You can build the project by running using the following different methods:
	
	1. COMMAND LINE:	mvn clean package
	
	2. M2ECLIPSE:		Right-click and select Run As --> Maven Build...
						Under Goals enter "clean package"
						Click Run
	
	The maven build produces the build artifacts under the ${project}/target folder.

	
= Setup =
	
	Prior to running any project executables, the following must be setup:
	
	1. $BIOR_CATALOG_HOME environment variable
	
	2. Tabix application
			
			Download tabix-0.2.5.tar.bz2 from  http://sourceforge.net/projects/samtools/files/tabix
			unzip tabix-0.2.5.tar.bz2 to your local harddrive (e.g. /Applications/tabix-0.2.5)
			cd /Applications/tabix-0.2.5
			make
			add the following to your shell profile: export PATH=/Applications/tabix-0.2.5:$PATH
	
= Running =
	
	TODO