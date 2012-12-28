#!/bin/bash
#------------------------------------------------------------------------------------------
# Assumptions:
# 1) BIOR_CATALOG_HOME variable has been set
# 2) Tabix is installed and on path
# 3) Even thought dbSNP provides the compressed file and tabix index as a download, it does NOT have
#    the json info we want.  Therefore we'll create a text file with the json object, re-zip the file,
#    then build the tabix index again on our new sorted text file containing the json objects.
#------------------------------------------------------------------------------------------

# If either of the two inputs is not specified, then exit
dbsnpRawInAll=$1
outDir=$2
if [ -z "$dbsnpRawInAll" ] || [ "${dbsnpRawInAll}" = "" ] || [ -z "$outDir" ] || [ "${outDir}" = "" ];
then
  echo "Usage:  publish_dbsnp.sh <dbsnpAllVariantsGzip> <outputDirectory>"; 
  echo "Ex:  publish_dbsnp.sh  /data/dbsnp/dbsnpAll.vcf.gz  /data/catalogs/dbsnp";
  exit;
fi

# Print each line that is executed (-x), and exit if any command fails (-e)
set -e

echo "Checking if environment variables are set..."
if [ -z "$BIOR_CATALOG_HOME" ] && [ "${BIOR_CATALOG_HOME+x}" = "x" ]; then
  echo "BIOR_CATALOG_HOME not set"
  echo "You many need to run:"
  echo "  cd bior_catalog"
  echo "  ./setenv.sh"
fi
if [ -z "$BIOR_LITE_HOME" ] && [ "${BIOR_LITE_HOME+x}" = "x" ]; then
  echo "BIOR_LITE_HOME not set"
  echo "You many need to run:"
  echo "  cd bior_pipeline"
  echo "  ./setenv.sh"
fi

### Create the gzip'd file from the original vcf.
### NOTE: Assumes that the original vcf is sorted by chromosome then position
### a) Extract all variants from the original dbsnp file
### b) Pipe into bior_vcf_to_json.sh which will add a JSON string to the end (containing all data)
### c) Pipe into bio_drill.sh to add the maxBP as the last column
### d) Pipe into grep and remove all header lines (lines beginning with "#")
### e) Pipe into cut which will save only the chrom, minBP, maxBP, JSON columns
### f) Pipe into bgzip to create a Bgzip compressed stream
### g) Output the bgzip compressed binary stream to a file
zcat $dbsnpRawInAll | bior_vcf_to_json.sh | bior_drill.sh -k -p "_maxBP" | grep -v "^#" | cut -f 1,2,9,10 | bgzip > $outDir/dbsnp.tsv.bgz

### Build Tabix index
tabix -s 1 -b 2  $outDir/dbsnp.tsv.bgz

echo "Done."
