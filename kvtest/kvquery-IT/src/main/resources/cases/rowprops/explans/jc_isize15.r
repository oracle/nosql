compiled-query-plan

{
"query file" : "rowprops/q/jc_isize15.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "GROUP",
    "input variable" : "$gb-1",
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "GROUP",
        "input variable" : "$gb-4",
        "input iterator" :
        {
          "iterator kind" : "RECEIVE",
          "distribution kind" : "ALL_SHARDS",
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
                "target table" : "Boo",
                "row variable" : "$f",
                "index used" : "idx_city_phones",
                "covering index" : true,
                "index row variable" : "$f_idx",
                "index scans" : [
                  {
                    "equality conditions" : {},
                    "range conditions" : {}
                  }
                ],
                "position in join" : 0
              },
              "FROM variable" : "$f_idx",
              "SELECT expressions" : [
                {
                  "field name" : "gb-0",
                  "field expression" : 
                  {
                    "iterator kind" : "FUNC_PARTITION",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$f_idx"
                    }
                  }
                },
                {
                  "field name" : "aggr-1",
                  "field expression" : 
                  {
                    "iterator kind" : "CONST",
                    "value" : 1
                  }
                },
                {
                  "field name" : "aggr-2",
                  "field expression" : 
                  {
                    "iterator kind" : "FUNC_MKINDEX_STORAGE_SIZE",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$f_idx"
                    }
                  }
                },
                {
                  "field name" : "aggr-3",
                  "field expression" : 
                  {
                    "iterator kind" : "FUNC_MKINDEX_STORAGE_SIZE",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$f_idx"
                    }
                  }
                },
                {
                  "field name" : "id_gen",
                  "field expression" : 
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "#id",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$f_idx"
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
                "field name" : "aggr-1",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$gb-2"
                }
              },
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "id_gen",
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
                  "field name" : "aggr-3",
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
          }
        },
        "grouping expressions" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "gb-0",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-4"
            }
          },
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-4"
            }
          },
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id_gen",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-4"
            }
          }
        ],
        "aggregate functions" : [
          {
            "iterator kind" : "FUNC_SUM",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "aggr-3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$gb-4"
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
                "variable" : "$gb-4"
              }
            }
          }
        ]
      },
      "FROM variable" : "$from-3",
      "SELECT expressions" : [
        {
          "field name" : "gb-0",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "gb-0",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-3"
            }
          }
        },
        {
          "field name" : "aggr-1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-3"
            }
          }
        },
        {
          "field name" : "aggr-2",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-3"
            }
          }
        },
        {
          "field name" : "aggr-3",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-3"
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
        "iterator kind" : "FUNC_COUNT_STAR"
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
        "iterator kind" : "FN_COUNT_NUMBERS",
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
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "part",
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
      "field name" : "cnt",
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
      "field name" : "index_size",
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