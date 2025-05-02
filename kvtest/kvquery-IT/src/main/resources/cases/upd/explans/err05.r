compiled-query-plan

{
"query file" : "upd/q/err05.q",
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
          "iterator kind" : "FIELD_STEP",
          "field name" : "areacode",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "phones",
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
                  "variable" : "$$u"
                }
              }
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
                "value" : 1000
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
        "row variable" : "$$u",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"id":0},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$u",
      "SELECT expressions" : [
        {
          "field name" : "u",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$u"
          }
        }
      ]
    }
  }
}
}
