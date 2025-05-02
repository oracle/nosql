compiled-query-plan

{
"query file" : "gb/q/noidx_sort10.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 2 ],
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
                "target table" : "Bar",
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
                  "field name" : "aggr-1",
                  "field expression" : 
                  {
                    "iterator kind" : "FN_SEQ_SUM",
                    "input iterator" :
                    {
                      "iterator kind" : "SEQ_MAP",
                      "mapper iterator" :
                      {
                        "iterator kind" : "MULTIPLY_DIVIDE",
                        "operations and operands" : [
                          {
                            "operation" : "*",
                            "operand" :
                            {
                              "iterator kind" : "FIELD_STEP",
                              "field name" : "price",
                              "input iterator" :
                              {
                                "iterator kind" : "VAR_REF",
                                "variable" : "$sq1"
                              }
                            }
                          },
                          {
                            "operation" : "*",
                            "operand" :
                            {
                              "iterator kind" : "FIELD_STEP",
                              "field name" : "qty",
                              "input iterator" :
                              {
                                "iterator kind" : "VAR_REF",
                                "variable" : "$sq1"
                              }
                            }
                          }
                        ]
                      },
                      "input iterator" :
                      {
                        "iterator kind" : "ARRAY_FILTER",
                        "input iterator" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "items",
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
                },
                {
                  "field name" : "aggr-2",
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
                  "field name" : "aggr-3",
                  "field expression" : 
                  {
                    "iterator kind" : "CONST",
                    "value" : 1
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
              }
            ],
            "aggregate functions" : [
              {
                "iterator kind" : "FUNC_SUM",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "aggr-1",
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
                  "field name" : "aggr-2",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$gb-2"
                  }
                }
              },
              {
                "iterator kind" : "FUNC_COUNT_STAR"
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
          }
        ],
        "aggregate functions" : [
          {
            "iterator kind" : "FUNC_SUM",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "aggr-1",
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
      "FROM variable" : "$from-1",
      "SELECT expressions" : [
        {
          "field name" : "prodcat",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "gb-0",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        },
        {
          "field name" : "sales",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        },
        {
          "field name" : "sort_gen",
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
                    "variable" : "$from-1"
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
                    "variable" : "$from-1"
                  }
                }
              }
            ]
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "prodcat",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "prodcat",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "sales",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "sales",
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