#!/bin/bash
MODES=(random probaasc probadesc hour00 hour06 hour12 hour18 commentsasc commentsdesc pointsasc pointsdesc probadesc0.1 probadesc0.5 probadesc0.1positive hierarchy fusion)
for mode in ${MODES[@]}; do
  tar cvzf $mode.tar.gz $mode/mongo/data
done
TIME=$(date +%Y%m%d%H%M%S)
ARCHIVES_DIR=archives/$TIME
mkdir -p $ARCHIVES_DIR
mv *.tar.gz $ARCHIVES_DIR
cd $ARCHIVES_DIR
tar cvf all.tar *.tar.gz 
