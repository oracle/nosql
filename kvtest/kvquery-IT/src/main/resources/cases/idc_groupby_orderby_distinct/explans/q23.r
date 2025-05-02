compiled-query-plan

{
"query file" : "idc_groupby_orderby_distinct/q/q23.q",
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
                "target table" : "ComplexType",
                "row variable" : "$$f",
                "index used" : "primary index",
                "covering index" : false,
                "index scans" : [
                  {
                    "equality conditions" : {"id":0},
                    "range conditions" : {}
                  },
                  {
                    "equality conditions" : {"id":1},
                    "range conditions" : {}
                  }
                ],
                "position in join" : 0
              },
              "FROM variable" : "$$f",
              "WHERE" : 
              {
                "iterator kind" : "AND",
                "input iterators" : [
                  {
                    "iterator kind" : "IN",
                    "left-hand-side expressions" : [
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "id",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$$f"
                        }
                      },
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "age",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$$f"
                        }
                      }
                    ],
                    "right-hand-side expressions" : [
                      [
                        {
                          "iterator kind" : "CONST",
                          "value" : 0
                        },
                        {
                          "iterator kind" : "CONST",
                          "value" : 10
                        }
                      ],
                      [
                        {
                          "iterator kind" : "CONST",
                          "value" : 1
                        },
                        {
                          "iterator kind" : "CONST",
                          "value" : 11
                        }
                      ]
                    ]
                  },
                  {
                    "iterator kind" : "ANY_GREATER_THAN",
                    "left operand" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "flt",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$f"
                      }
                    },
                    "right operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 4.5
                    }
                  }
                ]
              },
              "SELECT expressions" : [
                {
                  "field name" : "gb-0",
                  "field expression" : 
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "lng",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$f"
                    }
                  }
                },
                {
                  "field name" : "aggr-1",
                  "field expression" : 
                  {
                    "iterator kind" : "MULTIPLY_DIVIDE",
                    "operations and operands" : [
                      {
                        "operation" : "*",
                        "operand" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "age",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$$f"
                          }
                        }
                      },
                      {
                        "operation" : "*",
                        "operand" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "id",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$$f"
                          }
                        }
                      }
                    ]
                  }
                },
                {
                  "field name" : "aggr-2",
                  "field expression" : 
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "dbl",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$f"
                    }
                  }
                },
                {
                  "field name" : "aggr-3",
                  "field expression" : 
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "lng",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$f"
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
                "iterator kind" : "FN_COUNT",
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
          "field name" : "Column_1",
          "field expression" : 
          {
            "iterator kind" : "ADD_SUBTRACT",
            "operations and operands" : [
              {
                "operation" : "+",
                "operand" :
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
                "operation" : "+",
                "operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                }
              }
            ]
          }
        },
        {
          "field name" : "Column_2",
          "field expression" : 
          {
            "iterator kind" : "ADD_SUBTRACT",
            "operations and operands" : [
              {
                "operation" : "+",
                "operand" :
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
                "operation" : "+",
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
              }
            ]
          }
        },
        {
          "field name" : "sort_gen",
          "field expression" : 
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
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "Column_1",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "Column_1",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "Column_2",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "Column_2",
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