#!/bin/sh

# Script to create keystore and truststore using CA signed certificate
# This script assume you have a openssl CA setup. To set up a openssl CA,
# follow the guide below: 
# https://jamielinux.com/docs/openssl-certificate-authority/introduction.html

caconfig=openssl.cnf
rootcert=ca.cert.pem
intermeidatecert=intermediate.cert.pem
pw=123456

keytool -genkeypair -keystore store.keys -alias shared -keyAlg RSA \
-keySize 2048 -validity 36500 -storepass ${pw} \
-dname "CN=unittest, OU=NoSQL, O=Example, L=Unknown, ST=California, C=US"

keytool -certreq -keystore store.keys -alias shared -file unittest.csr \
-storepass ${pw}

openssl ca -config ${caconfig} -extensions server_cert \
-days 36500 -notext -md sha256 -in unittest.csr -out unittest.pem

rm -f unittest.csr

keytool -import -file ${rootcert} \
-keystore store.keys -alias root -storepass ${pw}

keytool -import -file ${intermeidatecert} \
-keystore store.keys -alias intermediate -storepass ${pw}

keytool -import -file unittest.pem \
-keystore store.keys -alias shared -storepass ${pw}

keytool -import -file ${rootcert} \
-keystore store.trust -alias root -storepass ${pw}

keytool -import -file ${intermeidatecert} \
-keystore store.trust -alias intermediate -storepass ${pw}

keytool -export -file shared.cert -keystore store.keys \
-alias shared -storepass ${pw}

keytool -import -keystore store.trust -file shared.cert -storepass ${pw}

rm -f shared.cert

cp store.trust error-store.trust

keytool -delete -keystore error-store.trust -alias mykey -storepass ${pw}

# export private key to key.pem
keytool -importkeystore -srckeystore store.keys -destkeystore key.p12 \
-deststoretype pkcs12 -srcstorepass ${pw} -deststorepass ${pw}
openssl pkcs12 -in key.p12  -nodes -nocerts -out key.pem -password pass:${pw}

# create a keystore converted with openssl
cat unittest.pem ${intermeidatecert} ${rootcert} > chain.pem
openssl pkcs12 -export -in chain.pem -inkey key.pem \
-out good.p12 -password pass:${pw} -name shared

keytool -importkeystore -srckeystore good.p12 -srcstoretype pkcs12 \
-destkeystore openssl-store.keys -deststoretype jks \
-srcstorepass ${pw} -deststorepass ${pw}

# create a error keystore that private key entry doesn't have chain
openssl pkcs12 -export -in unittest.pem -inkey key.pem \
-out error.p12 -password pass:${pw} -name shared

keytool -importkeystore -srckeystore error.p12 -srcstoretype pkcs12 \
-destkeystore error-store.keys -deststoretype jks \
-srcstorepass ${pw} -deststorepass ${pw}

rm -f key.pem
rm -f key.p12
rm -f good.p12
rm -f error.p12
rm -f unittest.pem
rm -f chain.pem