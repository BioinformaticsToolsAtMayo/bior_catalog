#!/bin/bash
#------------------------------------------------------------------------------------------
# Assumptions:
# 1) Bgzip is installed on the local machine and in $PATH
# 2) Tabix is installed on the local machine and in $PATH
#------------------------------------------------------------------------------------------
# Print each line that is executed (-x), and exit if any command fails (-e)
#set -x -e
set -e

currentDir="`pwd`/"
currentScript=`echo $0 | sed 's/^[./]*//'`
## echo "currentScript : $currentScript"
scriptPath="$currentDir$currentScript"
## echo "scriptPath : $scriptPath"
scriptDir="`dirname $scriptPath`"
## echo "scriptDir: $scriptDir"
echo "Running setupEnv.sh from: $scriptDir/../"
cd "$scriptDir/../"
source setupEnv.sh
cd $currentDir

#------------------------------------------------------------------------------------------
# Given raw data files (*.gbs.txt) build JSON data file (which contains position + JSON)
#------------------------------------------------------------------------------------------
rawDataDir=$1
targetCatalogDir=$2

if [ -z "$rawDataDir" ] || [ ! -d "$rawDataDir" ] ; then
  echo "No directory specified for RAW DATA (or directory does not exist): $rawDataDir"
  echo "Example:  /data5/bsi/refdata-new/ncbi_genome/human/downloaded/latest/2014_02_04/Assembled_chromosomes/gbs"
  echo "Where this directory would contain files (one per chromosome) like: hs_ref_GRCh37.p13_chr11.gbs.gz"
  exit 1;
fi  

if [ -z "$targetCatalogDir" ] || [ ! -d "$targetCatalogDir" ] ; then
  echo "No directory specified for TARGET CATALOG (or directory does not exist): $targetCatalogDir"
  exit 1;
fi  


echo "Build JSON from raw data files"
echo "Target directory: $targetCatalogDir"
java -cp $BIOR_CATALOG_HOME/conf:$BIOR_CATALOG_HOME/lib/* edu.mayo.bior.publishers.NCBIGene.NCBIGenePublisher $rawDataDir  $targetCatalogDir


#------------------------------------------------------------------------------------------
# Sort the JSON data file by columns 1 (chr-string), 2 (minBP-numeric), and 3 (maxBP-numeric), and bgzip it
#------------------------------------------------------------------------------------------
echo "Sort and bgzip the genes JSON data file..."
sort -k 1,1 -k 2,2n -k 3,3n  $targetCatalogDir/genes.tsv  |  bgzip >  $targetCatalogDir/genes.tsv.bgz


#------------------------------------------------------------------------------------------
# Create Tabix index
# s = landmark, b = begin position, e = end position
#------------------------------------------------------------------------------------------
echo "Create tabix index on the bgzip file..."
tabix -s 1 -b 2 -e 3  $targetCatalogDir/genes.tsv.bgz

#------------------------------------------------------------------------------------------
# Remove the temporary genes.tsv file that NCBIGenePublisher created
#------------------------------------------------------------------------------------------
echo "Remove temp files..."
rm $targetCatalogDir/genes.tsv

#------------------------------------------------------------------------------------------
# Create index on Gene Id
# Ex: Index <bgzipDataFile> <delimiter> <keyColumn> <isKeyColumnInt> <bgzipIndexOut>
#------------------------------------------------------------------------------------------
##### java -cp $BIOR_CATALOG_HOME/conf:$BIOR_CATALOG_HOME/lib/*  edu.mayo.bior.indexer.cmd.BuildBgzipIndex  


echo "DONE."
