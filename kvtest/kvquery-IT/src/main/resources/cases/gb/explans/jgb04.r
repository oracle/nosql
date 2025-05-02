compiled-query-plan

{
"query file" : "gb/q/jgb04.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 0, 1 ],
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
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$f",
      "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "acctno",
          "field expression" : 
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
          }
        },
        {
          "field name" : "year",
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
          "field name" : "count",
          "field expression" : 
          {
            "iterator kind" : "FUNC_COUNT_STAR"
          }
        },
        {
          "field name" : "sales",
          "field expression" : 
          {
            "iterator kind" : "FUNC_SUM",
            "input iterator" :
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
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
  "SELECT expressions" : [
    {
      "field name" : "acctno",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "acctno",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "year",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "year",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "count",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "count",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    },
    {
      "field name" : "sales",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "sales",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    }
  ]
}
}