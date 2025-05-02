compiled-query-plan

{
"query file" : "time/q/arith_misc01.q",
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
          "equality conditions" : {"id":2},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$arithtest",
    "SELECT expressions" : [
      {
        "field name" : "DIFF_T0_T3",
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
                  "variable" : "$$arithtest"
                }
              }
            }
          ]
        }
      },
      {
        "field name" : "DURATION_T0_T3",
        "field expression" : 
        {
          "iterator kind" : "FN_GET_DURATION",
          "input iterator" :
          {
            "iterator kind" : "FN_TIMESTAMP_DIFF",
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
                    "variable" : "$$arithtest"
                  }
                }
              }
            ]
          }
        }
      },
      {
        "field name" : "RET_T0",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "tm0",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$arithtest"
            }
          },
          "right operand" :
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
                    "variable" : "$$arithtest"
                  }
                }
              },
              {
                "iterator kind" : "FN_GET_DURATION",
                "input iterator" :
                {
                  "iterator kind" : "FN_TIMESTAMP_DIFF",
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
                          "variable" : "$$arithtest"
                        }
                      }
                    }
                  ]
                }
              }
            ]
          }
        }
      },
      {
        "field name" : "DIFF_T3_T0",
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
                "iterator kind" : "FIELD_STEP",
                "field name" : "tm3",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$arithtest"
                }
              }
            },
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
            }
          ]
        }
      },
      {
        "field name" : "DURATION_T3_T0",
        "field expression" : 
        {
          "iterator kind" : "FN_GET_DURATION",
          "input iterator" :
          {
            "iterator kind" : "FN_TIMESTAMP_DIFF",
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
                    "variable" : "$$arithtest"
                  }
                }
              },
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
              }
            ]
          }
        }
      },
      {
        "field name" : "RET_T3",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "tm3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$arithtest"
            }
          },
          "right operand" :
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
                "iterator kind" : "FN_GET_DURATION",
                "input iterator" :
                {
                  "iterator kind" : "FN_TIMESTAMP_DIFF",
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
                          "variable" : "$$arithtest"
                        }
                      }
                    },
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
                    }
                  ]
                }
              }
            ]
          }
        }
      }
    ]
  }
}
}