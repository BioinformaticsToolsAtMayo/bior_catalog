#!/bin/bash
echo 'Publishing NCBI Gene to: ' $1 ;
java -cp $BIOR_CATALOG_HOME/conf:$BIOR_CATALOG_HOME/lib/* edu.mayo.publishers.NCBIGene.NCBIGenePublisher $@

