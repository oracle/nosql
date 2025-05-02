compiled-query-plan

{
"query file" : "gb/q/distinct18.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-1",
  "input iterator" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 0 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "GROUP",
        "input variable" : "$gb-3",
        "input iterator" :
        {
          "iterator kind" : "RECEIVE",
          "distribution kind" : "ALL_PARTITIONS",
          "input iterator" :
          {
            "iterator kind" : "GROUP",
            "input variable" : "$gb-2",
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
                    "equality conditions" : {},
                    "range conditions" : {}
                  }
                ],
                "position in join" : 0
              },
              "FROM variable" : "$$f",
              "SELECT expressions" : [
                {
                  "field name" : "gb-0",
                  "field expression" : 
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "year",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "xact",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$f"
                      }
                    }
                  }
                },
                {
                  "field name" : "gb-1",
                  "field expression" : 
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "prodcat",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "xact",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$f"
                      }
                    }
                  }
                },
                {
                  "field name" : "aggr-2",
                  "field expression" : 
                  {
                    "iterator kind" : "FN_SEQ_SUM",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "price",
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "item",
                        "input iterator" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "xact",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$$f"
                          }
                        }
                      }
                    }
                  }
                },
                {
                  "field name" : "aggr-3",
                  "field expression" : 
                  {
                    "iterator kind" : "FN_SEQ_COUNT_NUMBERS_I",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "price",
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "item",
                        "input iterator" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "xact",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$$f"
                          }
                        }
                      }
                    }
                  }
                }
              ]
            },
            "grouping expressions" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "gb-0",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$gb-2"
                }
              },
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "gb-1",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$gb-2"
                }
              }
            ],
            "aggregate functions" : [
              {
                "iterator kind" : "FUNC_SUM",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "aggr-2",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$gb-2"
                  }
                }
              },
              {
                "iterator kind" : "FUNC_SUM",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "aggr-3",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$gb-2"
                  }
                }
              }
            ]
          }
        },
        "grouping expressions" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "gb-0",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-3"
            }
          },
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "gb-1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-3"
            }
          }
        ],
        "aggregate functions" : [
          {
            "iterator kind" : "FUNC_SUM",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "aggr-2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$gb-3"
              }
            }
          },
          {
            "iterator kind" : "FUNC_SUM",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "aggr-3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$gb-3"
              }
            }
          }
        ]
      },
      "FROM variable" : "$from-0",
      "SELECT expressions" : [
        {
          "field name" : "avg",
          "field expression" : 
          {
            "iterator kind" : "MULTIPLY_DIVIDE",
            "operations and operands" : [
              {
                "operation" : "*",
                "operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "aggr-2",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$from-0"
                  }
                }
              },
              {
                "operation" : "div",
                "operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "aggr-3",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$from-0"
                  }
                }
              }
            ]
          }
        }
      ]
    }
  },
  "grouping expressions" : [
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "avg",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-1"
      }
    }
  ],
  "aggregate functions" : [

  ]
}
}