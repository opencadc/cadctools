#!/bin/bash
if [[ $1 == "status" ]]; then
    # status does not require namecheck
    /usr/local/bin/cadc-etrans $@ /input
else
    # add the namecheck
    /usr/local/bin/cadc-etrans $@ -c /root/.config/cadc/namecheck.xml /input
fi
