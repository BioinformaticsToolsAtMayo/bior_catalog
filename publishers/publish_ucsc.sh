#!/bin/bash
inDir=$1
outDir=$2

if [ $# -eq 0 ] ; then
    echo 'This script requires two directories: '
    echo '1: the location of the ucsc data e.g. : /data/ucsc/hg19/'
    echo '2: the location of the output e.g. : /tmp/test/'
    echo 'run the script like this: '
    echo '~/workspace/bior_catalog/publishers/publish_ucsc.sh /data/ucsc/hg19/ /tmp/test/'
    echo 'also make sure you have tabix and bgzip in your path'
    exit 0
fi

mkdir ${outDir}/scratch

echo 'Running UCSC Publisher...';
java -cp $BIOR_CATALOG_HOME/conf:$BIOR_CATALOG_HOME/lib/* edu.mayo.bior.publishers.UCSC.UCSCPublisher $inDir $outDir 


#export LC_ALL=C ;
for i in $( ls $outDir ); do   
    echo sorting:  ${outDir}$i;
    sort -k1,1 -k2,2n ${outDir}$i > ${outDir}/scratch/$i  
    echo compressing: ${outDir}/scratch/$i ;
    bgzip ${outDir}/scratch/$i ;
    echo removing published data file:  ${outDir}$i; 
    rm ${outDir}$i ;
done

echo moving catalogs into place: ${outDir}/scratch/
cd ${outDir}/scratch/
echo `pwd`
echo 'renaming files'
rename 's/\.gz$/\.bgz/' *1.tsv.gz
rename 's/\.gz$/\.bgz/' *2.tsv.gz
rename 's/\.gz$/\.bgz/' *.tsv.gz

echo 'tabix all bgz files'
for i in $( ls | grep ".tsv.bgz"); do   
    echo tabix:  ${outDir}/scratch/$i;
    tabix -s 1 -b 2 -e 3 ${outDir}/scratch/$i;
done

echo 'moving files'
mv *1.tsv.bgz.tbi ..
mv *2.tsv.bgz.tbi ..
mv *.tsv.bgz.tbi ..

mv *1.tsv.bgz ..
mv *2.tsv.bgz ..
mv *.tsv.bgz ..

echo 'done'
