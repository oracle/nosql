#!/bin/bash

VERBOSE=0
if [ "$1" = "-v" ] ; then
  VERBOSE=1
  shift
fi

PORT=$1
[ "$PORT" = "" ] && PORT=8080


URL="http://localhost:$PORT/20190828"
#COMPARTMENT="ocid1.compartment.oc1..aaaaaaaagaqos5k"
COMPARTMENT="testCompartment"
TABLENAME="testTable"
AUTH="Authorization: Bearer foo"
CTYPE="Content-Type: application/json"

trap "/bin/rm -f *.json.$$ *.out.$$" EXIT

function docurl() {
  curl -s -S -vvv -H "$AUTH" "$@" > curl.out.$$ 2>&1
  [ $? -ne 0 ] && cat curl.out.$$ && exit 1
  # grep for 200 OK in verbose output
  egrep 'HTTP.* 200 OK|HTTP.* 202 Accepted' curl.out.$$ > /dev/null
  [ $? -ne 0 ] && egrep '^{|^< HTTP/' curl.out.$$ && exit 1
  [ $VERBOSE -eq 1 ] && grep '^{' curl.out.$$ | grep -v 'bytes data'
  [ $VERBOSE -eq 1 ] && echo ""
  [ $VERBOSE -eq 1 ] && echo ""
}

cat > create.json.$$ << EOT
{
 "id": 1,
 "ifNotExists": true,
 "name": "$TABLENAME",
 "compartmentId": "$COMPARTMENT",
 "ddlStatement": "create table if not exists $TABLENAME(id integer, name string, age integer, info json, primary key(id))",
 "tableLimits": {"maxReadUnits": 500, "maxWriteUnits": 500, "maxStorageInGBs": 5, "capacityMode": "PROVISIONED"}
}
EOT
# Create the table
[ $VERBOSE -eq 1 ] && echo "creating table $TABLENAME..."
docurl "$URL/tables" -H "$CTYPE" --data-binary @create.json.$$
# wait a bit for it to be created

sleep 10
# see if the table exists
docurl "$URL/tables/$TABLENAME?compartmentId=$COMPARTMENT" -X GET

ID=5
cat > put.json.$$ << EOT
{
 "compartmentId": "$COMPARTMENT",
 "value": {
   "id": $ID,
   "name": "John",
   "age": 54,
   "info": "{\"lastname\":\"Smith\", \"street\":\"1234 main street\"}"
 }
}
EOT
# Put a row
[ $VERBOSE -eq 1 ] && echo "Putting a row with id=$ID"
docurl "$URL/tables/$TABLENAME/rows" -H "$CTYPE" -X PUT --data-binary @put.json.$$

# get the row
[ $VERBOSE -eq 1 ] && echo "Getting row with id=$ID"
docurl "$URL/tables/$TABLENAME/rows?compartmentId=$COMPARTMENT&key=id:$ID" -X GET

cat > query.json.$$ << EOT
{
 "compartmentId": "$COMPARTMENT",
 "statement":"select * from $TABLENAME where age > 50 and age < 60",
 "timeoutInMs": 1000
}
EOT

# query for the row
[ $VERBOSE -eq 1 ] && echo "Querying for rows in age range"
docurl "$URL/query" -H "$CTYPE" --data-binary @query.json.$$

#delete the row
[ $VERBOSE -eq 1 ] && echo "Deleting row with id=$ID"
docurl "$URL/tables/$TABLENAME/rows?compartmentId=$COMPARTMENT&key=id:$ID" -X DELETE

# query again
[ $VERBOSE -eq 1 ] && echo "Querying for rows in age range"
docurl "$URL/query" -H "$CTYPE" --data-binary @query.json.$$

# delete the table
[ $VERBOSE -eq 1 ] && echo "Deleting table $TABLENAME"
docurl "$URL/tables/$TABLENAME?compartmentId=$COMPARTMENT" -X DELETE

[ $VERBOSE -eq 1 ] && echo "Test successful."
exit 0
