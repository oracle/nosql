compiled-query-plan

{
"query file" : "gb/q/sort04.q",
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
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "RECEIVE",
          "distribution kind" : "ALL_SHARDS",
          "order by fields at positions" : [ 0 ],
          "input iterator" :
          {
            "iterator kind" : "SELECT",
            "FROM" :
            {
              "iterator kind" : "TABLE",
              "target table" : "Foo",
              "row variable" : "$$f",
              "index used" : "idx_long_bool",
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
            "GROUP BY" : "Grouping by the first expression in the SELECT list",
            "SELECT expressions" : [
              {
                "field name" : "gb-0",
                "field expression" : 
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "long",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "record",
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
                  "iterator kind" : "FUNC_SUM",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "int",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "record",
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
                "field name" : "aggr-2",
                "field expression" : 
                {
                  "iterator kind" : "FUNC_COUNT_STAR"
                }
              }
            ]
          }
        },
        "FROM variable" : "$from-2",
        "GROUP BY" : "Grouping by the first expression in the SELECT list",
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
                "variable" : "$from-2"
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
                  "variable" : "$from-2"
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
                  "variable" : "$from-2"
                }
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
          "field name" : "long",
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
                  "field name" : "aggr-1",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$from-1"
                  }
                }
              },
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
      "field name" : "long",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "long",
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