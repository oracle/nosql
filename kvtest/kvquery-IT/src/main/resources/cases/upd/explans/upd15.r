compiled-query-plan

{
"query file" : "upd/q/upd15.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "UPDATE_ROW",
      "indexes to update" : [  ],
      "update clauses" : [
        {
          "iterator kind" : "REMOVE",
          "clone new values" : false,
          "theIsMRCounterDec" : false,
          "theJsonMRCounterColPos" : -1,
          "theIsJsonMRCounterUpdate" : false,
          "target iterator" :
          {
            "iterator kind" : "VALUES",
            "predicate iterator" :
            {
              "iterator kind" : "OR",
              "input iterators" : [
                {
                  "iterator kind" : "EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$value"
                  },
                  "right operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 10
                  }
                },
                {
                  "iterator kind" : "EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$value"
                  },
                  "right operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 20
                  }
                }
              ]
            },
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "maps",
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
            "iterator kind" : "KEYS",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "children",
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
              "equality conditions" : {"id":14},
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
