compiled-query-plan

{
"query file" : "idc_groupby_orderby_distinct/q/q10.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 2 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "GROUP",
      "input variable" : "$gb-2",
      "input iterator" :
      {
        "iterator kind" : "RECEIVE",
        "distribution kind" : "ALL_PARTITIONS",
        "input iterator" :
        {
          "iterator kind" : "GROUP",
          "input variable" : "$gb-1",
          "input iterator" :
          {
            "iterator kind" : "SELECT",
            "FROM" :
            {
              "iterator kind" : "TABLE",
              "target table" : "ComplexType",
              "row variable" : "$$p",
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
            "FROM variable" : "$$p",
            "WHERE" : 
            {
              "iterator kind" : "GREATER_OR_EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "age",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$p"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 0
              }
            },
            "SELECT expressions" : [
              {
                "field name" : "gb-0",
                "field expression" : 
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "flt",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$p"
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
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "age",
                    "input iterator" :
                    {
                      "iterator kind" : "VALUES",
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "children",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$$p"
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
                            "field name" : "home",
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
                            "field name" : "work",
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
                        "field name" : "phones",
                        "input iterator" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "address",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$$p"
                          }
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
                            "field name" : "home",
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
                            "field name" : "work",
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
                        "field name" : "phones",
                        "input iterator" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "address",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$$p"
                          }
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
                "variable" : "$gb-1"
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
                  "variable" : "$gb-1"
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
                  "variable" : "$gb-1"
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
                  "variable" : "$gb-1"
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
    },
    "FROM variable" : "$from-0",
    "SELECT expressions" : [
      {
        "field name" : "flt",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "gb-0",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-0"
          }
        }
      },
      {
        "field name" : "childAge",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "aggr-1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-0"
          }
        }
      },
      {
        "field name" : "Column_3",
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
}
}