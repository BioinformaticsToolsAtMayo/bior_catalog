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
echo "BIOR_CATALOG_HOME=$BIOR_CATALOG_HOME"
#source $BIOR_CATALOG_HOME/setupEnv.sh


#------------------------------------------------------------------------------------------
# Given a raw data files (*.gbs.txt) build JSON data file (which contains position + JSON)
#------------------------------------------------------------------------------------------
rawDataDir=$1
targetCatalogDir=$2
ncbiGenomeFile=$3

echo "Build JSON from raw data files"
echo "Target directory: $targetCatalogDir"
java -cp $BIOR_CATALOG_HOME/conf:$BIOR_CATALOG_HOME/lib/* edu.mayo.bior.publishers.Cosmic.CosmicPublisher $rawDataDir  $targetCatalogDir $ncbiGenomeFile


#------------------------------------------------------------------------------------------
# Sort the JSON data file by columns 1 (chr-string), 2 (minBP-numeric), and 3 (maxBP-numeric), and bgzip it
#------------------------------------------------------------------------------------------
echo "Sort and bgzip..."
sort -k 1,1 -k 2,2n -k 3,3n  $targetCatalogDir/scratch/cosmic.tsv  |  bgzip >  $targetCatalogDir/cosmic.tsv.bgz


#------------------------------------------------------------------------------------------
# Create Tabix index
# s = landmark, b = begin position, e = end position
#------------------------------------------------------------------------------------------
echo "Create tabix index on the bgzip file..."
tabix -s 1 -b 2 -e 3  $targetCatalogDir/cosmic.tsv.bgz

#------------------------------------------------------------------------------------------
# Remove the temporary genes.tsv file that NCBIGenePublisher created
#------------------------------------------------------------------------------------------
echo "Remove temp files..."
rm $targetCatalogDir/scratch/cosmic.tsv
rmdir $targetCatalogDir/scratch

echo "DONE."
