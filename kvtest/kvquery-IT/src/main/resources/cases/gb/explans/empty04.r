compiled-query-plan

{
"query file" : "gb/q/empty04.q",
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
          "target table" : "Foo",
          "row variable" : "$$f",
          "index used" : "primary index",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : { "id1" : { "end value" : 0, "end inclusive" : false } }
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$f",
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
                "field name" : "discount",
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
            "field name" : "aggr-1",
            "field expression" : 
            {
              "iterator kind" : "FUNC_COUNT_STAR"
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
                "field name" : "id3",
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
              "iterator kind" : "FN_COUNT_NUMBERS",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "id3",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f"
                }
              }
            }
          },
          {
            "field name" : "aggr-4",
            "field expression" : 
            {
              "iterator kind" : "FUNC_COLLECT",
              "distinct" : false,
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "discount",
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
          "iterator kind" : "FUNC_COLLECT",
          "distinct" : false,
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
      }
    ]
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "sum",
      "field expression" : 
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
    },
    {
      "field name" : "collect",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "aggr-4",
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