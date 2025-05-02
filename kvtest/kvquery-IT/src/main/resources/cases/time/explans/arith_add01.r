compiled-query-plan

{
"query file" : "time/q/arith_add01.q",
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
      "row variable" : "$$arithtest",
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
    "FROM variable" : "$$arithtest",
    "SELECT expressions" : [
      {
        "field name" : "tm0",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "tm0",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$arithtest"
          }
        }
      },
      {
        "field name" : "P1Y",
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
                  "variable" : "$$arithtest"
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "1 year"
            }
          ]
        }
      },
      {
        "field name" : "SUB_P6M",
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
                  "variable" : "$$arithtest"
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "-6 months"
            }
          ]
        }
      },
      {
        "field name" : "P30D",
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
                  "variable" : "$$arithtest"
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "30 days"
            }
          ]
        }
      },
      {
        "field name" : "P1Y6M14D",
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
                  "variable" : "$$arithtest"
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "1 year 6 months 14 days"
            }
          ]
        }
      },
      {
        "field name" : "SUB_P1Y6M14D",
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
                  "variable" : "$$arithtest"
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "- 1 year 6 months 14 days"
            }
          ]
        }
      }
    ]
  }
}
}