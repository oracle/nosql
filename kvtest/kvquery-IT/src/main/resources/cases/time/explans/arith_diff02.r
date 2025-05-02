compiled-query-plan

{
"query file" : "time/q/arith_diff02.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "arithtest",
      "row variable" : "$t",
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
    "FROM variable" : "$t",
    "SELECT expressions" : [
      {
        "field name" : "DIFF1",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_DIFF",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "2021-11-27T00:00:00.000000000Z"
            },
            {
              "iterator kind" : "CAST",
              "target type" : "Timestamp(9)",
              "quantifier" : "?",
              "input iterator" :
              {
                "iterator kind" : "CAST",
                "target type" : "String",
                "quantifier" : "",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "tm0",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$t"
                  }
                }
              }
            }
          ]
        }
      },
      {
        "field name" : "DIFF2",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_DIFF",
          "input iterators" : [
            {
              "iterator kind" : "CAST",
              "target type" : "Timestamp(9)",
              "quantifier" : "?",
              "input iterator" :
              {
                "iterator kind" : "CAST",
                "target type" : "String",
                "quantifier" : "",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "tm0",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$t"
                  }
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "2021-11-27T00:00:00.000000000Z"
            }
          ]
        }
      },
      {
        "field name" : "DIFF3",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_DIFF",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "1970-01-02T00:00:00.000000000Z"
            },
            {
              "iterator kind" : "CONST",
              "value" : "1970-01-01T00:00:00.000000000Z"
            }
          ]
        }
      },
      {
        "field name" : "DIFF4",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_DIFF",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "2020-03-01T00:00:00.000000000Z"
            },
            {
              "iterator kind" : "CAST",
              "target type" : "Timestamp(9)",
              "quantifier" : "?",
              "input iterator" :
              {
                "iterator kind" : "PROMOTE",
                "target type" : "Any",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "tm3",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "info",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$t"
                    }
                  }
                }
              }
            }
          ]
        }
      }
    ]
  }
}
}