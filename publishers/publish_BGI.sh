rawDir=/data/BGI/hg18
catalogDir=/data/catalogs/BGI/hg19
scratch=$catalogDir/scratch
liftOverDir=/data/ucsc/exe

### Create the scratch directory if it doesn't already exist
if [ ! -f $FILE ];
then
  mkdir $scratch
fi

### Cut out all the extra patient columns at the end of the genotype file
### remove the header, then sort
zcat $rawDir/LuCAMP_200exomeFinal.genotype.gz | grep -v "#" | cut -f 1-4 | sort -k1,1 -k2,2n  >  $scratch/1.genotype.part.sorted.tsv

### Sort the maf file
zcat $rawDir/LuCAMP_200exomeFinal.maf.gz | sort -k1,1 -k2,2n  > $scratch/1.maf.sorted.tsv

### Combine the maf and genotype files,
### duplicate the minBP to use as maxBP col and add that after the minBP,
### quote the allele frequency column (that looks like a float),
### add minBP on the end so we have the original hg18 data (liftover will modify the first few position columns)
### keep only the refAllele and isDbSNP columns from genotype file
### Format:
###  chr  .  .  minBP  maxBP  majorAlleleIndex  minorAlleleIndex  ACount  CCount  GCount  TCount  alleleFreq  minBpPreLiftoverMaf minBpPreLiftoverGenotype  refAllele  isInDbSNP
echo -e "###chr\t.\t.\tminBP\tmaxBP\tmajIdx\tminIdx\tAs\tCs\tGs\tTs\tAlleleFreqs\tminBpPreLiftoverMaf\tminBpPreLiftoverGenotype\tRefAllele\tisInDbSNP"  >  $scratch/2.mafPlusRef.tsv
paste  $scratch/1.maf.sorted.tsv  $scratch/1.genotype.part.sorted.tsv  \
  | awk -F \\t '{print $1 FS "." FS "." FS $2 FS $2 FS $3 FS $4 FS $5 FS $6 FS $7 FS $8 FS "\""$9"\"" FS $2 FS $11 FS $12 FS $13}'  \
  >>  $scratch/2.mafPlusRef.tsv

### Liftover the BGI minBP & maxBP hg18 to hg19
$liftOverDir/liftOver -gff  $scratch/2.mafPlusRef.tsv  $liftOverDir/hg18ToHg19.over.chain  $scratch/3.mafPlusRef.liftedOver.tsv  $scratch/3.unmapped.txt

### Re-sort the file, but with the minBP treated as a numeric column
sort -k1,1 -k4,4n $scratch/3.mafPlusRef.liftedOver.tsv > $scratch/4.mafPlusRef.liftedOver.sorted.tsv

### Call BGIPublisher to convert the majorIdx/minIdx to base-pairs, and to create the JSON column
java -cp $BIOR_CATALOG_HOME/conf:$BIOR_CATALOG_HOME/lib/* edu.mayo.bior.publishers.BGIDanish.BGIPublisher $scratch/4.mafPlusRef.liftedOver.sorted.tsv $scratch/5.catalog.json.tsv


### Build the final bgz and tabix files
bgzip -c $scratch/5.catalog.json.tsv  >  $catalogDir/LuCAMP_200exomeFinal_hg19.tsv.bgz
tabix -s 1 -b 2 -e 3 $catalogDir/LuCAMP_200exomeFinal_hg19.tsv.bgz
