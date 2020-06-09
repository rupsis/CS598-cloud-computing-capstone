# Pipe “>>” appends to final´s destination file, preserving old contents (and original files)
if [ -d $1 ]
then
for K in $1/*.csv ; do cat $K >> $1/compressedData.csv ; done 
else
  echo "this file is not a directory"
fi
