compiled-query-plan
{
"query file" : "idc_multirow_update/q/q15.q",
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
          "field name" : "phones",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_FILTER",
            "predicate iterator" :
            {
              "iterator kind" : "IS_OF_TYPE",
              "target types" : [
                {
                "type" : { "Array" : 
                  "Any"
                },
                "quantifier" : "",
                "only" : false
                }
              ],
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "phones",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$element"
                }
              }
            },
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "address",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            }
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "areacode"
            },
            {
              "iterator kind" : "EXTERNAL_VAR_REF",
              "variable" : "$areacode"
            },
            {
              "iterator kind" : "CONST",
              "value" : "number"
            },
            {
              "iterator kind" : "EXTERNAL_VAR_REF",
              "variable" : "$number"
            },
            {
              "iterator kind" : "CONST",
              "value" : "kind"
            },
            {
              "iterator kind" : "EXTERNAL_VAR_REF",
              "variable" : "$kind"
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
          "field name" : "phones",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_FILTER",
            "predicate iterator" :
            {
              "iterator kind" : "IS_OF_TYPE",
              "target types" : [
                {
                "type" : { "Map" : 
                  "Any"
                },
                "quantifier" : "",
                "only" : false
                }
              ],
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "phones",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$element"
                }
              }
            },
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "address",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            }
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : false,
          "input iterators" : [
            {
              "iterator kind" : "VALUES",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$"
              }
            },
            {
              "iterator kind" : "MAP_CONSTRUCTOR",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : "areacode"
                },
                {
                  "iterator kind" : "EXTERNAL_VAR_REF",
                  "variable" : "$areacode"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "number"
                },
                {
                  "iterator kind" : "EXTERNAL_VAR_REF",
                  "variable" : "$number"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "kind"
                },
                {
                  "iterator kind" : "EXTERNAL_VAR_REF",
                  "variable" : "$kind"
                }
              ]
            }
          ]
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
        "target table" : "jsoncol",
        "row variable" : "$$j",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"majorKey1":"j1","majorKey2":"j2"},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$j",
      "SELECT expressions" : [
        {
          "field name" : "j",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$j"
          }
        }
      ]
    }
  }
}
}
