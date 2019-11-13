#!/bin/bash

function joinBy {
    local d=$1; 
    shift; 
    echo -n "$1"; 
    shift; 
    printf "%s" "${@/#/$d}";
}
function httpParam {
    echo "$1" | sed 's/[&?]/\n/g' | grep "$2=" | sed s/$2=// |
            sed -e's/%\([0-9A-F][0-9A-F]\)/\\\\\x\1/g' | \
            xargs echo -e
}
function adcpHeader {
    echo "Content-type: text/html"
    echo ""
    echo "Filename=$1"
    echo "MimeType=$2"
    if [ "$3" != "" ]
    then
        echo "Encoding=$3"
    fi
}
function adcpTrailer {
    echo "TRAILER"
    echo "ReturnStatus=$1"
    if [ "$2" != "" ]
    then
        echo "StdError="
        echo "$2"
    fi
    echo "ENDTRAILER"
}

