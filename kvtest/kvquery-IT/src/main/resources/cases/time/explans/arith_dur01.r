compiled-query-plan

{
"query file" : "time/q/arith_dur01.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "arithtest",
      "row variable" : "$$arithtest",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$arithtest",
    "WHERE" : 
    {
      "iterator kind" : "OP_IS_NOT_NULL",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "tm3",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$arithtest"
        }
      }
    },
    "SELECT expressions" : [
      {
        "field name" : "DUR",
        "field expression" : 
        {
          "iterator kind" : "FN_GET_DURATION",
          "input iterator" :
          {
            "iterator kind" : "CAST",
            "target type" : "Long",
            "quantifier" : "",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "tm3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$arithtest"
              }
            }
          }
        }
      },
      {
        "field name" : "RET",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FN_TIMESTAMP_ADD",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : "1970-01-01T00:00:00.000000000Z"
              },
              {
                "iterator kind" : "FN_GET_DURATION",
                "input iterator" :
                {
                  "iterator kind" : "CAST",
                  "target type" : "Long",
                  "quantifier" : "",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "tm3",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$arithtest"
                    }
                  }
                }
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "tm3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$arithtest"
            }
          }
        }
      }
    ]
  }
}
}