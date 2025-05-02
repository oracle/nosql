#!/bin/sh

# Script to set up the default unit testing OAuth keystore and truststore

keystore=oauth.keys
truststore=oauth.trust
certfile=key.cert
pw=unittest
signingKeyalias=signingkey
wrongKeyalias=wrongkey
pwfile=oauth.passwd

rm ${keystore}

# make self-signed signing key pair
keytool -genkeypair -keystore ${keystore} -storepass ${pw} -keypass ${pw} \
        -alias ${signingKeyalias} -dname 'cn="Unit Test"' -keyAlg RSA \
        -validity 36500

rm ${certfile}

# export cert
keytool -export -alias ${signingKeyalias} -file ${certfile} \
        -keystore ${keystore} -storepass ${pw}

rm ${truststore}

# import cert into truststore
keytool -import -alias ${signingKeyalias} -file ${certfile} \
        -keystore ${truststore} -storepass ${pw} -noprompt

rm ${certfile}

# make self-signed wrong key pair
keytool -genkeypair -keystore ${keystore} -storepass ${pw} -keypass ${pw} \
        -alias ${wrongKeyalias} -dname 'cn="Unit Test"' -keyAlg RSA \
        -validity 36500

rm ${certfile}

# export cert
keytool -export -alias ${wrongKeyalias} -file ${certfile} \
        -keystore ${keystore} -storepass ${pw}

rm ${truststore}

# import cert into truststore
keytool -import -alias ${wrongKeyalias} -file ${certfile} \
        -keystore ${truststore} -storepass ${pw} -noprompt

rm ${certfile}

# Create a password store file that tracks the store password
cat > ${pwfile} <<EOF
Password Store:
secret.keystore=${pw}
EOF
