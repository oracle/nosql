#!/bin/sh

# Script to set up the default unit testing SSL truststore after merged
# store.trust and other.trust, this script must be executed after make-ks.sh
# make-other-ks.sh.

mergetrusts=merge.trust
storetrusts=store.trust
othertrusts=other.trust
certfile=trust.cert
trustalias=shared

storepwd=unittest
otherpwd=othertest
mergepwd=unittest

rm ${mergetrusts}
rm ${certfile}

# export store cert
keytool -export -file ${certfile} -keystore ${storetrusts} \
        -storepass ${storepwd} -alias ${trustalias}

# import store cert into merged truststore
keytool -import -alias mykey_1 -file ${certfile} \
        -keystore ${mergetrusts} -storepass ${mergepwd} \
        -storetype PKCS12 -noprompt

rm ${certfile}

# export other cert
keytool -export -file ${certfile} -keystore ${othertrusts} \
        -storepass ${otherpwd} -alias ${trustalias} 

# import other cert into merged truststore
keytool -import -alias mykey -file ${certfile} \
        -keystore ${mergetrusts} -storepass ${mergepwd} \
        -storetype PKCS12 -noprompt

rm ${certfile}
