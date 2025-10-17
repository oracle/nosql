compiled-query-plan
{
"query file" : "idc_multirow_update/q/q4.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "UPDATE_ROW",
    "indexes to update" : [  ],
    "update clauses" : [
      {
        "iterator kind" : "ADD",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "friends",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "CONST",
          "value" : "Jerry"
        }
      },
      {
        "iterator kind" : "PUT",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "hobbies"
            },
            {
              "iterator kind" : "ARRAY_CONSTRUCTOR",
              "conditional" : false,
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : "Cooking"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "Music"
                }
              ]
            }
          ]
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
          "field name" : "street",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "address",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          }
        }
      }
    ],
    "update TTL" : false,
    "isCompletePrimaryKey" : false,
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "users",
        "row variable" : "$$t",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"sid1":3,"sid2":4,"pid1":3},
            "range conditions" : { "pid2" : { "end value" : 3, "end inclusive" : false } }
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$t",
      "SELECT expressions" : [
        {
          "field name" : "t",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      ]
    }
  }
}
}
