compiled-query-plan

{
"query file" : "upd/q/upd10.q",
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
        "iterator kind" : "SET",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "ARRAY_FILTER",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "values",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f"
              }
            }
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "CASE",
          "clauses" : [
            {
              "when iterator" :
              {
                "iterator kind" : "IS_OF_TYPE",
                "target types" : [
                  {
                  "type" : "Number",
                  "quantifier" : "",
                  "only" : false
                  }
                ],
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$"
                }
              },
              "then iterator" :
              {
                "iterator kind" : "ADD_SUBTRACT",
                "operations and operands" : [
                  {
                    "operation" : "+",
                    "operand" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$"
                    }
                  },
                  {
                    "operation" : "+",
                    "operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 10
                    }
                  }
                ]
              }
            },
            {
              "when iterator" :
              {
                "iterator kind" : "IS_OF_TYPE",
                "target types" : [
                  {
                  "type" : "String",
                  "quantifier" : "",
                  "only" : false
                  }
                ],
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$"
                }
              },
              "then iterator" :
              {
                "iterator kind" : "SEQ_CONCAT",
                "input iterators" : [

                ]
              }
            },
            {
              "when iterator" :
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
                  "iterator kind" : "VAR_REF",
                  "variable" : "$"
                }
              },
              "then iterator" :
              {
                "iterator kind" : "MAP_CONSTRUCTOR",
                "input iterators" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : "new"
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : "map"
                  }
                ]
              }
            },
            {
              "else iterator" :
              {
                "iterator kind" : "CONST",
                "value" : "????"
              }
            }
          ]
        }
      }
    ],
    "update TTL" : false,
    "isCompletePrimaryKey" : true,
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Foo",
        "row variable" : "$$f",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"id":9},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$f",
      "SELECT expressions" : [
        {
          "field name" : "f",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
          }
        }
      ]
    }
  }
}
}
