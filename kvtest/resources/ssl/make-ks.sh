#!/bin/sh

# Script to set up the default unit testing SSL keystore and truststore

keystore=store.keys
jkskeystore=store.keys.jks
truststore=store.trust
othertruststore=store.trust.other
jkstruststore=store.trust.jks
clienttrust=client.trust.pass
jksclienttrust=client.trust.jks
certfile=key.cert
pw=unittest
sharedalias=shared
pwfile=store.passwd
otherpw=othertest

rm ${keystore}

# make self-signed key pair
keytool -genkeypair -keystore ${keystore} -storepass ${pw} -keypass ${pw} \
        -alias ${sharedalias} -dname 'cn="Unit Test"' -keyAlg RSA \
        -validity 36500 -storetype PKCS12

rm ${certfile}

# export cert
keytool -export -alias ${sharedalias} -file ${certfile} -keystore ${keystore} \
        -storepass ${pw}

rm ${truststore} ${othertruststore}

# import cert into truststore
keytool -import -alias ${sharedalias} -file ${certfile} \
        -keystore ${truststore} -storepass ${pw} -storetype PKCS12 -noprompt

# import cert into another trustore with different password
# used by SSLSystemTest
keytool -import -alias ${sharedalias} -file ${certfile} \
        -keystore ${othertruststore} -storepass ${otherpw} \
        -storetype PKCS12 -noprompt

rm ${clienttrust} ${jksclienttrust}

# import cert into a password-protected PKCS12 truststore
keytool -import -alias ${sharedalias} -file ${certfile} \
        -keystore ${clienttrust} -storepass ${pw} \
        -storetype PKCS12 -noprompt

# import cert into a JKS truststore
keytool -import -alias ${sharedalias} -file ${certfile} \
        -keystore ${jksclienttrust} -storepass ${pw} -storetype JKS -noprompt

rm ${certfile}

rm ${jkskeystore}

# make self-signed keypair with JKS keystore
keytool -genkeypair -keystore ${jkskeystore} -storepass ${pw} -keypass ${pw} \
        -alias ${sharedalias} -dname 'cn="Unit Test"' -keyAlg RSA \
        -validity 36500 -storetype JKS

rm ${certfile}

# export cert
keytool -export -alias ${sharedalias} -file ${certfile} \
        -keystore ${jkskeystore} -storepass ${pw}

rm ${jkstruststore}

# import cert into JKS truststore
keytool -import -alias ${sharedalias} -file ${certfile} \
        -keystore ${jkstruststore} -storepass ${pw} \
        -storetype PKCS12 -noprompt

# Create a password store file that tracks the store password
cat > ${pwfile} <<EOF
Password Store:
secret.keystore=${pw}
EOF
