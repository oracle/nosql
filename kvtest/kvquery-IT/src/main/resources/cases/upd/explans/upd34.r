compiled-query-plan

{
"query file" : "upd/q/upd34.q",
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
          "iterator kind" : "SET",
          "clone new values" : false,
          "theIsMRCounterDec" : false,
          "theJsonMRCounterColPos" : -1,
          "theIsJsonMRCounterUpdate" : false,
          "target iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "age",
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
          "new value iterator" :
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
                  "value" : 2
                }
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
            "field name" : "lastName",
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
          "new value iterator" :
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$ext1"
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
              "equality conditions" : {"id":28},
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
