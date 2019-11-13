#!/bin/bash -f 
#
# This script invokes fcat to perform the actual cutout. A header is written
# to the output stream, followed by the data stream, followed by a trailer
# which will include status and error information.
#
# file=   : Physical path of the fits file to be cutout using fcat.
# head=   : Boolean indicating if this is a HEADER only request.  This variable will
#     be set to 'true' if this is a HEADER request, 'false' otherwise.
# fileid= : Logical filename to be used for setting header fields
# cutout= : Input parameters for fcat as provided by the user. For example:
#     [36][1:100,1:100]
# 

thisDir=${0%/*}

if [ "$thisDir" = "fslice2" ] ; then
    thisDir = "."
fi

. $thisDir/adcpbits.sh

cutoutCommand=cadccutout
reblockCmd=reblock

shopt -s extglob 

# parameters with values URL decoded
# file
filePath=$(httpParam $QUERY_STRING file )
#head
headerRequest=$(httpParam $QUERY_STRING head)
#fileid
logicalFilename=$(httpParam $QUERY_STRING  fileid)

#remove head and file arguments to get whitespace delimited string of cutouts
cutoutkey=$(httpParam $QUERY_STRING  cutout)
firstChar=${cutoutkey:0:1}
cutoutParameters=($cutoutkey) #array of cutouts
if test "${firstChar}" = "["; then
    cutoutParameters=$(joinBy '' ${cutoutParameters[@]})
else
    cutoutParameters=$(joinBy ' ' ${cutoutParameters[@]})
fi

set pipefail

exec 3>&1

# Calculate the output file name. The file will be uncompressed, so strip off
# any .fz or .gz extension. The file will be a fits file.

# Create a string representation of the cutout parameters that is safe
# to use as a file name or URI.  Replace all non-numeric characters
# with an underscore character.

# Turn parameters into a long string
# Replace non numerics with an underscore
cutoutkey=`echo ${cutoutkey//[^0-9a-zA-Z]/_}`

if test "${firstChar}" = "["; then
    # Remove the leading and trailing underscore
    cutoutlen=`echo ${#cutoutkey}`
    cutoutkey=`echo ${cutoutkey:1:cutoutlen-2}`
fi

# Strip off any leading path or trailing compression
outputFile="${logicalFilename%%?(.gz|.fz)}"

# Make the output file name: 
#  {everything up to the last dot}.{hashvalue}.{everthing after the last dot}
# This should turn something like x.fits into x.<hash>.fits
if test "${cutoutkey}" != "" ; then
    if test "${outputFile##*.}" = "${outputFile}"; then
	outputFile="${outputFile%.*}.${cutoutkey}"
    else
	outputFile="${outputFile%.*}.${cutoutkey}.${outputFile##*.}"
    fi
fi

# Write the header
adcpHeader $outputFile "application/fits" "x-fits"

if test "${headerRequest}" = "true"; then
    adcpTrailer 0 
    exit 0
fi

# Write the fits file(s)
runCommand="${cutoutCommand} --infile ${filePath} --outfile - \"$cutoutParameters\""
fcatstderr=$( { ${cutoutCommand} --infile ${filePath} --outfile - "$cutoutParameters" | ${reblockCmd} 1>&3; } 2>&1 )
fcatStatus=$?

if test ${#fcatstderr} -ne 0 -a $fcatStatus -eq 0 ; then
    fcatStatus=-1
fi

# Write the trailer
adcpTrailer $fcatStatus "$fcatstderr"

exit 0
