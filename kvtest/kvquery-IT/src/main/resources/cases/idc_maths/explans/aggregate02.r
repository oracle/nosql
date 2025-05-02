compiled-query-plan

{
"query file" : "idc_maths/q/aggregate02.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_PARTITIONS",
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "aggregate_test",
          "row variable" : "$$aggregate_test",
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
        "FROM variable" : "$$aggregate_test",
        "GROUP BY" : "No grouping expressions",
        "SELECT expressions" : [
          {
            "field name" : "aggr-0",
            "field expression" : 
            {
              "iterator kind" : "FUNC_SUM",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "value2",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$aggregate_test"
                }
              }
            }
          },
          {
            "field name" : "aggr-1",
            "field expression" : 
            {
              "iterator kind" : "FN_COUNT_NUMBERS",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "value2",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$aggregate_test"
                }
              }
            }
          },
          {
            "field name" : "aggr-2",
            "field expression" : 
            {
              "iterator kind" : "FUNC_SUM",
              "input iterator" :
              {
                "iterator kind" : "ABS",
                "input iterators" : [
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "value2",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$aggregate_test"
                    }
                  }
                ]
              }
            }
          },
          {
            "field name" : "aggr-3",
            "field expression" : 
            {
              "iterator kind" : "FN_COUNT_NUMBERS",
              "input iterator" :
              {
                "iterator kind" : "ABS",
                "input iterators" : [
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "value2",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$aggregate_test"
                    }
                  }
                ]
              }
            }
          },
          {
            "field name" : "aggr-4",
            "field expression" : 
            {
              "iterator kind" : "FUNC_SUM",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "value3",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$aggregate_test"
                }
              }
            }
          },
          {
            "field name" : "aggr-5",
            "field expression" : 
            {
              "iterator kind" : "FN_COUNT_NUMBERS",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "value3",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$aggregate_test"
                }
              }
            }
          },
          {
            "field name" : "aggr-6",
            "field expression" : 
            {
              "iterator kind" : "FUNC_SUM",
              "input iterator" :
              {
                "iterator kind" : "ROUND",
                "input iterators" : [
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "value3",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$aggregate_test"
                    }
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : 1
                  }
                ]
              }
            }
          },
          {
            "field name" : "aggr-7",
            "field expression" : 
            {
              "iterator kind" : "FN_COUNT_NUMBERS",
              "input iterator" :
              {
                "iterator kind" : "ROUND",
                "input iterators" : [
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "value3",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$aggregate_test"
                    }
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : 1
                  }
                ]
              }
            }
          }
        ]
      }
    },
    "FROM variable" : "$from-1",
    "GROUP BY" : "No grouping expressions",
    "SELECT expressions" : [
      {
        "field name" : "aggr-0",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-0",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        }
      },
      {
        "field name" : "aggr-1",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        }
      },
      {
        "field name" : "aggr-2",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        }
      },
      {
        "field name" : "aggr-3",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
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
      },
      {
        "field name" : "aggr-4",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-4",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        }
      },
      {
        "field name" : "aggr-5",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-5",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        }
      },
      {
        "field name" : "aggr-6",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-6",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        }
      },
      {
        "field name" : "aggr-7",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-7",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        }
      }
    ]
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "absavg",
      "field expression" : 
      {
        "iterator kind" : "ROUND",
        "input iterators" : [
          {
            "iterator kind" : "ABS",
            "input iterators" : [
              {
                "iterator kind" : "MULTIPLY_DIVIDE",
                "operations and operands" : [
                  {
                    "operation" : "*",
                    "operand" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "aggr-0",
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
                      "field name" : "aggr-1",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$from-0"
                      }
                    }
                  }
                ]
              }
            ]
          },
          {
            "iterator kind" : "CONST",
            "value" : 2
          }
        ]
      }
    },
    {
      "field name" : "avgabs",
      "field expression" : 
      {
        "iterator kind" : "ROUND",
        "input iterators" : [
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
          },
          {
            "iterator kind" : "CONST",
            "value" : 2
          }
        ]
      }
    },
    {
      "field name" : "roundavg",
      "field expression" : 
      {
        "iterator kind" : "ROUND",
        "input iterators" : [
          {
            "iterator kind" : "ROUND",
            "input iterators" : [
              {
                "iterator kind" : "MULTIPLY_DIVIDE",
                "operations and operands" : [
                  {
                    "operation" : "*",
                    "operand" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "aggr-4",
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
                      "field name" : "aggr-5",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$from-0"
                      }
                    }
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 1
              }
            ]
          },
          {
            "iterator kind" : "CONST",
            "value" : 2
          }
        ]
      }
    },
    {
      "field name" : "avground",
      "field expression" : 
      {
        "iterator kind" : "ROUND",
        "input iterators" : [
          {
            "iterator kind" : "MULTIPLY_DIVIDE",
            "operations and operands" : [
              {
                "operation" : "*",
                "operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "aggr-6",
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
                  "field name" : "aggr-7",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$from-0"
                  }
                }
              }
            ]
          },
          {
            "iterator kind" : "CONST",
            "value" : 2
          }
        ]
      }
    }
  ]
}
}