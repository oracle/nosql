compiled-query-plan

{
"query file" : "gb/q/seq_sort06.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "GROUP",
    "input variable" : "$gb-2",
    "input iterator" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_SHARDS",
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
            "target table" : "Foo",
            "row variable" : "$$f",
            "index used" : "idx_acc_year_prodcat",
            "covering index" : false,
            "index row variable" : "$$f_idx",
            "index scans" : [
              {
                "equality conditions" : {},
                "range conditions" : {}
              }
            ],
            "index filtering predicate" :
            {
              "iterator kind" : "EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "xact.year",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f_idx"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 2000
              }
            },
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
                "iterator kind" : "MAP_CONSTRUCTOR",
                "input iterators" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : "acctno"
                  },
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "acctno",
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
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : "amount"
                  },
                  {
                    "iterator kind" : "MULTIPLY_DIVIDE",
                    "operations and operands" : [
                      {
                        "operation" : "*",
                        "operand" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "qty",
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
                      },
                      {
                        "operation" : "*",
                        "operand" :
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
                    ]
                  }
                ]
              }
            },
            {
              "field name" : "aggr-2",
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
              "variable" : "$gb-1"
            }
          }
        ],
        "aggregate functions" : [
          {
            "iterator kind" : "FUNC_COLLECT",
            "distinct" : false,
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
          "variable" : "$gb-2"
        }
      }
    ],
    "aggregate functions" : [
      {
        "iterator kind" : "FUNC_COLLECT",
        "distinct" : false,
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
      }
    ]
  },
  "FROM variable" : "$from-0",
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
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "accounts",
      "field expression" : 
      {
        "iterator kind" : "ARRAY_CONSTRUCTOR",
        "conditional" : true,
        "input iterators" : [
          {
            "iterator kind" : "FN_SEQ_SORT",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_FILTER",
              "input iterator" :
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
          }
        ]
      }
    },
    {
      "field name" : "cnt",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "aggr-2",
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