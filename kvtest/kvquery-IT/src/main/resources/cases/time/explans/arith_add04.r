compiled-query-plan

{
"query file" : "time/q/arith_add04.q",
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
        "field name" : "T0",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_ADD",
          "input iterators" : [
            {
              "iterator kind" : "CAST",
              "target type" : "Timestamp(9)",
              "quantifier" : "?",
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
            },
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "duration",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$t"
              }
            }
          ]
        }
      },
      {
        "field name" : "T3",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_ADD",
          "input iterators" : [
            {
              "iterator kind" : "CAST",
              "target type" : "Timestamp(9)",
              "quantifier" : "?",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "tm3",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$t"
                }
              }
            },
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "duration",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$t"
              }
            }
          ]
        }
      },
      {
        "field name" : "T9",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_ADD",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "tm9",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$t"
              }
            },
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "duration",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$t"
              }
            }
          ]
        }
      },
      {
        "field name" : "JT3",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_ADD",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "duration",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$t"
              }
            }
          ]
        }
      },
      {
        "field name" : "JT6",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_ADD",
          "input iterators" : [
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
                  "field name" : "tm6",
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
            },
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "duration",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$t"
              }
            }
          ]
        }
      }
    ]
  }
}
}