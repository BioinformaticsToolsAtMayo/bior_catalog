#!/usr/bin/perl -w
use strict;
use LWP::Simple;
my $url = "http://www.genenames.org/cgi-bin/hgnc_downloads.cgi?".
    "title=HGNC%20output%20data&hgnc_dbtag=on&col=gd_hgnc_id&col=gd_app_sym&col=gd_app_name&".
    "col=gd_status&col=gd_locus_type&col=gd_locus_group&col=gd_prev_sym&col=gd_prev_name&".
    "col=gd_aliases&col=gd_name_aliases&col=gd_pub_chrom_map&col=gd_date2app_or_res&col=gd_date_mod&".
    "col=gd_date_sym_change&col=gd_date_name_change&col=gd_pub_acc_ids&col=gd_enz_ids&col=gd_pub_eg_id&".
    "col=gd_pub_ensembl_id&col=gd_mgd_id&col=gd_other_ids&col=gd_other_ids_list&col=gd_pubmed_ids&".
    "col=gd_pub_refseq_ids&col=gd_gene_fam_name&col=gd_gene_fam_pagename&col=gd_record_type&col=gd_primary_ids&".
    "col=gd_secondary_ids&col=gd_ccds_ids&col=gd_vega_ids&col=gd_lsdb_links&col=md_gdb_id&".
    "col=md_eg_id&col=md_mim_id&col=md_refseq_id&col=md_prot_id&col=md_ensembl_id&".
    "col=md_ucsc_id&col=md_mgd_id&col=md_rgd_id&status=Approved&status=Entry%20Withdrawn&".
    "status_opt=2&where=&order_by=gd_app_sym_sort&format=text&limit=&".
    "submit=submit&.cgifields=&.cgifields=status&.cgifields=chr&.cgifields=hgnc_dbtag";
my $page = get($url);
print $page;
