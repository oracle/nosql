compiled-query-plan
{
"query file" : "idc_multirow_update/q/q14.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "UPDATE_ROW",
    "indexes to update" : [ "idx_index" ],
    "update clauses" : [
      {
        "iterator kind" : "PUT",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$j"
        },
        "new value iterator" :
        {
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "address"
            },
            {
              "iterator kind" : "MAP_CONSTRUCTOR",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : "city"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "Burlington"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "State"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "MA"
                }
              ]
            }
          ]
        }
      },
      {
        "iterator kind" : "SET",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "firstThread",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$j"
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "CONST",
          "value" : true
        }
      },
      {
        "iterator kind" : "REMOVE",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "index",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$j"
          }
        }
      }
    ],
    "update TTL" : true,
    "TimeUnit" : "DAYS",
    "TTL iterator" :
    {
      "iterator kind" : "CONST",
      "value" : 8
    },
    "isCompletePrimaryKey" : false,
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "jsoncol",
        "row variable" : "$j",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"majorKey1":"k1","majorKey2":"k2"},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$j",
      "SELECT expressions" : [
        {
          "field name" : "j",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$j"
          }
        }
      ]
    }
  }
}
}
