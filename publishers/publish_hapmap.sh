# make sure you have the liftover tool installed in in your path...

#!/bin/bash
#------------------------------------------------------------------------------------------
# Assumptions:
# 1) BIOR_CATALOG_HOME variable has been set
# 2) Tabix is installed and on path
# 3) UCSC's liftover is installed in on your path at $UCSC_TOOLS_HOME
#------------------------------------------------------------------------------------------

# If either of the two inputs is not specified, then exit
hapmapDir=$1
outDir=$2
if [ -z "$hapmapDir" ] || [ "${hapmapDir}" = "" ] || [ -z "$outDir" ] || [ "${outDir}" = "" ];
then
  echo "Usage:  publish_hapmap.sh <hapmapdirectory> <outputDirectory>";
  echo "Ex:  publish_hapmap.sh  /data/hapmap/2010-08_phaseII+III/ /data/catalogs/hapmap/2010-08_phaseII+III";
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
if [ -z "$UCSC_TOOLS_HOME" ] && [ "${UCSC_TOOLS_HOME+x}" = "x" ]; then
  echo "$UCSC_TOOLS_HOME not set"
  echo "You many need to run:"
  echo "  export UCSC_TOOLS_HOME=/path/to/your/ucsc/commands/like/liftOver"
fi
##e.g. /data/ucsc/exe/macOSX.i386

echo 'Running HapMap Publisher Phase 1/6...';
#java -cp $BIOR_CATALOG_HOME/conf:$BIOR_CATALOG_HOME/lib/* edu.mayo.bior.publishers.HapMap.HapMapPublisher $0 $@ 

echo 'Converting hapmap.tsv to sorted gff, Phase 2/6...';
#cat ${outDir}/scratch/hapmap.tsv | bior_drill.sh -k -p chrom -p source -p type -p pos -p pos -p score -p strand -p phase | grep -v ^# | sort -k1,1 -k4,4n > ${outDir}/scratch/hapmap.sorted.tsv

echo 'Performing liftOver, Phase 3/6...'
$UCSC_TOOLS_HOME/liftOver -gff ${outDir}/scratch/hapmap.sorted.tsv  $UCSC_TOOLS_HOME/hg18toHg19.over.chain ${outDir}/scratch/hapmap.sorted.liftover.tsv ${outDir}/scratch/unmapped.tsv

echo 'Merging the records, Phase 4/6...'
java -cp $BIOR_CATALOG_HOME/conf:$BIOR_CATALOG_HOME/lib/* edu.mayo.bior.publishers.HapMap.HapMapPublisherPhase2 ${outDir}/scratch/hapmap.sorted.liftover.tsv  ${outDir}/scratch/hapmap.collapsedJson.tsv

echo 'Do we need to sort here before compressing and building tabix?????'

echo 'Compressing the catalog, Phase 5/6...'
java -cp $BIOR_CATALOG_HOME/conf:$BIOR_CATALOG_HOME/lib/* edu.mayo.bior.indexer.cmd.BuildBgzip ${outDir}/scratch/hapmap.collapsedJson.tsv  ${outDir}/allele_freqs.tsv.bgz

echo 'Building Tabix Index, Phase 6/6...'
### Build Tabix index
tabix -s 1 -b 2  $outDir/allele_freqs.tsv.bgz

echo "Done."
