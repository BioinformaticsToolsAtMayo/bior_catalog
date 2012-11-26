#!/bin/bash

#=========================================================================
# 5.b Download (this should be old, so use refdata-new space for these files) 
#=========================================================================
# Ensure that you havcd e 80 megabytes available in this directory. 
# Download ftp://ftp.ncbi.nlm.nih.gov/genomes/H_sapiens/Assembled_chromosomes/gbs/hs_ref_GRCh??.p?_chr??.gbs.gz
#To do this, on crick:
#	% ftp ftp.ncbi.nlm.nih.gov
#	Userid: anonymous
#	Password: your email address
#	% cd genomes/H_sapiens/Assembled_chromosomes/gbs
#	% prompt
#	% bin
#	% mget hs_ref_GRCh37.p2_chr*.gbs.gz - (it is now pr9, not p2)
#	% cd 
#	% cd gene/DATA
#	% get gene2refseq.gz 
#	% get gene_info.gz
#	% get gene_history.gz
#	% get gene2go.gz (Used by another BORA function but nice to get at same time as rest of data)
#	When you get all 24 files, you are done so you can exit:
#	% bye
#
# Source Dates on files downloaded on :
# gene_info: Jul 24 17:47
# gene2refseq:  Jul 24 17:41
# gene_history: Jul 24 17:43 
# gene2go:  Jul 24 17:45 
# hs_ref_GRCh37.p2_chr*.gbs.gz:  Nov 22  2010

echo 'Downloading Files to: ' $1 ;
mkdir -p $1 ;
cd $1 ;
pwd ;
wget ftp://ftp.ncbi.nlm.nih.gov/genomes/H_sapiens/Assembled_chromosomes/gbs/hs_ref_GRCh*.gbs.gz ;
wget ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_info.gz
wget ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2refseq.gz
wget ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_history.gz
wget ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2go.gz
