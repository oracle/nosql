compiled-query-plan

{
"query file" : "upd/q/upd28.q",
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
            "iterator kind" : "FIELD_STEP",
            "field name" : "info",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        },
        "position iterator" :
        {
          "iterator kind" : "CONST",
          "value" : 1
        },
        "new value iterator" :
        {
          "iterator kind" : "ARRAY_FILTER",
          "predicate iterator" :
          {
            "iterator kind" : "GREATER_THAN",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "areacode",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$element"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 400
            }
          },
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$"
          }
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
            "equality conditions" : {"id":23},
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
