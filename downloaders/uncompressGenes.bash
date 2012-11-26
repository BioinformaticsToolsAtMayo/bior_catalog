#!/bin/bash
#=========================================================================
# Uncompress the downloaded files. 
#=========================================================================

# This will uncompress the content of all of the hs_ref_GRChrXX.pX_chr*.fa.gz files 
# that you downloaded, and create chrX.gbs.txt files for them.

echo 'Unzipping Files From: ' $1 ;
mkdir -p $1 ;
echo 'Unzipping Files to: ' $2 ;
mkdir -p $2 ;
cd $2 ;
pwd ;
cp $1/*.gz . ;
gunzip *.gz ;
rename 's/hs_ref_.*_//' *.gbs ;
rename 's/gbs/gbs.txt/' *.gbs ;
grep "^9606" gene2refseq > human_gene2refseq.txt ; rm gene2refseq ;
grep "^9606" gene_history > human_gene_history.txt ; rm gene_history ; 
grep "^9606" gene_info > human_gene_info.txt ; rm gene_info ;
# remove some files we don't currently care about...
rm unlocalized.gbs.txt ;
rm unplaced.gbs.txt ;
rm alts.gbs.txt 
