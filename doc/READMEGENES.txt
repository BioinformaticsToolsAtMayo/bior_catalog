

#=========================================================================
# 1. Install Dependancies
#=========================================================================
a. Install java/javac and make sure it is in your path.
b. Install maven.  
On ubuntu it is simple: 
> sudo apt-get install maven2

b. Install wget, it may already be installed to check type:
> which wget

it should return something like:
/opt/local/bin/wget

c. you may also need to install 'rename' so the linux scripts work.  One
way is to install macports then do:
  >  sudo port install
  >  sudo port install p5-file-rename


#=========================================================================
# 2. Build the project
#=========================================================================

> mvn clean install
> mvn assembly:simple

#=========================================================================
# 3. Download the data
#=========================================================================
Select a location to place the raw zipped files e.g. /tmp/geneDownload/

> bin/downloadGenes.bash /tmp/geneDownload/

Note that BIOR production currently runs on: /data4/bsi/BIOR/s111579.prod

There is some good info in the script itself, if you need more details on
what this script is doing.

#=========================================================================
# 4. Extract the data
#=========================================================================
You have to know where the raw zipped files are e.g. /tmp/geneDownload
and you have to specify a directory where the unzipped stuff is going to go
e.g.  /tmp/NCBIGene.  Then do:

> bin/uncompressGenes.bash /tmp/geneDownload /tmp/NCBIGene

In production, we tend to put this raw info into: $BIOR_CATALOG_HOME/NCBIGene/version/scratch

Again, look inside the unzip script to see what is going on.  The result should look like this:

> bin/uncompress.bash /tmp/geneDownload /tmp/NCBIGene
Unzipping Files From:  /tmp/geneDownload
Unzipping Files to:  /tmp/NCBIGene
/tmp/NCBIGene
> ls /tmp/NCBIGene/
alts.gbs.txt		chr12.gbs.txt		chr16.gbs.txt		chr2.gbs.txt		chr3.gbs.txt		chr7.gbs.txt		chrY.gbs.txt		gene_info
chr1.gbs.txt		chr13.gbs.txt		chr17.gbs.txt		chr20.gbs.txt		chr4.gbs.txt		chr8.gbs.txt		gene2go			unlocalized.gbs.txt
chr10.gbs.txt		chr14.gbs.txt		chr18.gbs.txt		chr21.gbs.txt		chr5.gbs.txt		chr9.gbs.txt		gene2refseq		unplaced.gbs.txt
chr11.gbs.txt		chr15.gbs.txt		chr19.gbs.txt		chr22.gbs.txt		chr6.gbs.txt		chrX.gbs.txt		gene_history
> 
