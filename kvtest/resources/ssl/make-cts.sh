#!/bin/sh

# Script to set up the default unit testing client password-less PKCS12
# truststore. This script must be executed after make-ks.sh, make-other-ks.sh,
# make-merged-ts.sh.
#
# Must use Java 11.0.12 or higher versions that support PKCS12 password-less
# KeyStore for client.trust creation.
#
# Add following properties to java.security file before run this script.
# keystore.pkcs12.certProtectionAlgorithm = NONE
# keystore.pkcs12.macAlgorithm = NONE

truststore=store.trust
othertrusts=other.trust
clienttrust=client.trust
otherclienttrust=other.client.trust
mergeclienttrust=merge.client.trust
certfile=key.cert
sharedalias=shared
pw=unittest
otherpw=othertest

rm ${clienttrust} ${otherclienttrust} ${mergeclienttrust}
rm ${certfile}

# export cert
keytool -export -alias ${sharedalias} -file ${certfile} \
        -keystore ${truststore} -storepass ${pw}

# import cert into a password-less PKCS12 truststore
keytool -import -alias mykey -file ${certfile} \
        -keystore ${clienttrust}  -storetype PKCS12 \
        -noprompt

# import store cert into merged client truststore
keytool -import -alias mykey -file ${certfile} \
        -keystore ${mergeclienttrust} -storetype PKCS12 -noprompt

rm ${certfile}

# export cert
keytool -export -alias ${sharedalias} -file ${certfile} \
        -keystore ${othertrusts} -storepass ${otherpw}

# import cert into a password-less PKCS12 truststore
keytool -import -alias mykey_1 -file ${certfile} \
        -keystore ${otherclienttrust} -storetype PKCS12 -noprompt

# import store cert into merged client truststore
keytool -import -alias mykey_1 -file ${certfile} \
        -keystore ${mergeclienttrust} -storetype PKCS12 -noprompt

rm ${certfile}
