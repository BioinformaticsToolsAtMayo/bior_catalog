#!/bin/bash
#------------------------------------------------------------------------------------------
# Assumptions:
# 1) sys.properties will be loaded and the properties contain the correct path to the chr.gbs.txt files
# 2) Tabix is installed on the local machine
# 3) BIOR_CATALOG_HOME variable has been set
#------------------------------------------------------------------------------------------
# Print each line that is executed (-x), and exit if any command fails (-e)
#set -x -e
set -e
###../setupEnv.sh


#------------------------------------------------------------------------------------------
# Given a raw data files (*.gbs.txt) build JSON data file (which contains position + JSON)
#------------------------------------------------------------------------------------------
rawDataDir=$1
targetCatalogDir=$2

echo "Build JSON data file from raw data files in current directory"
echo "Assumes paths from sys.properties file have been loaded"
echo "Target directory: $targetCatalogDir"
java -cp $BIOR_CATALOG_HOME/conf:$BIOR_CATALOG_HOME/lib/* edu.mayo.bior.publishers.NCBIGene.NCBIGenePublisher $rawDataDir  $targetCatalogDir


#------------------------------------------------------------------------------------------
# Sort the JSON data file by columns 1 (chr-string), 2 (minBP-numeric), and 3 (maxBP-numeric), and bgzip it
#------------------------------------------------------------------------------------------
sort -k 1,1 -k 2,2n -k 3,3n  $targetCatalogDir/genes.tsv  |  bgzip >  $targetCatalogDir/genes.tsv.bgz


#------------------------------------------------------------------------------------------
# Create Tabix index
# s = landmark, b = begin position, e = end position
#------------------------------------------------------------------------------------------
tabix -s 1 -b 2 -e 3  $targetCatalogDir/genes.tsv.bgz

#------------------------------------------------------------------------------------------
# Remove the temporary genes.tsv file that NCBIGenePublisher created
#------------------------------------------------------------------------------------------
rm $targetCatalogDir/genes.tsv

#------------------------------------------------------------------------------------------
# Create index on Gene Id
# Ex: Index <bgzipDataFile> <delimiter> <keyColumn> <isKeyColumnInt> <bgzipIndexOut>
#------------------------------------------------------------------------------------------
##### java -cp $BIOR_CATALOG_HOME/conf:$BIOR_CATALOG_HOME/lib/*  edu.mayo.bior.indexer.cmd.BuildBgzipIndex  
