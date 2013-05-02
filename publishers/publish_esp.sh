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
#echo $BIOR_CATALOG_HOME
#source $BIOR_CATALOG_HOME/setupEnv.sh


#------------------------------------------------------------------------------------------
# Given a raw data files (*.vcf) build JSON data file (which contains position + JSON)
#------------------------------------------------------------------------------------------
rawDataDir=$1
targetCatalogDir=$2

echo "Build JSON from raw data files"
echo "Target directory: $targetCatalogDir"
java -cp $BIOR_CATALOG_HOME/conf:$BIOR_CATALOG_HOME/lib/* edu.mayo.bior.publishers.ESP.ESPPublisher $rawDataDir  $targetCatalogDir/scratch/ESP6500SI_GRCh37.tsv

echo "DONE."
