
rawDataFile=$1
catalogFile=$2

### Call MirBase to create the JSON column and rip out those columns that we don't need.
echo "Call MirBasePublisher to construct the JSON from the various columns..."
java -cp $BIOR_CATALOG_HOME/conf:$BIOR_CATALOG_HOME/lib/* edu.mayo.bior.publishers.MIRBase.MirBasePublisher $rawDataFile $catalogFile

### Build the final bgz and tabix files
echo "Build the bgz and tabix catalog files..."
bgzip -c  $catalogFile 
tabix -s 1 -b 2 -e 3 $catalogFile
