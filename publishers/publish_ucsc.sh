#!/bin/bash
inDir=$1
outDir=$2
mkdir ${outDir}/scratch

echo 'Running UCSC Publisher...';
java -cp $BIOR_CATALOG_HOME/conf:$BIOR_CATALOG_HOME/lib/* edu.mayo.bior.publishers.UCSC.UCSCPublisher $inDir $outDir 


#export LC_ALL=C ;
for i in $( ls $outDir ); do   
    echo sorting:  ${outDir}$i;
    sort -k1,1 -k2,2n ${outDir}$i > ${outDir}/scratch/$i  
    echo compressing: ${outDir}/scratch/$i ;
    bgzip ${outDir}/scratch/$i ;
done
