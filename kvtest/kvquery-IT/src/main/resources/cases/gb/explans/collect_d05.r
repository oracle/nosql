compiled-query-plan

{
"query file" : "gb/q/collect_d05.q",
"plan" : 
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
        "target table" : "Bar",
        "row variable" : "$$f",
        "index used" : "idx_state",
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
          "field name" : "state",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "state",
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
          "field name" : "amounts",
          "field expression" : 
          {
            "iterator kind" : "FUNC_COLLECT",
            "distinct" : true,
            "input iterator" :
            {
              "iterator kind" : "ARRAY_CONSTRUCTOR",
              "conditional" : false,
              "input iterators" : [
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
                          "field name" : "qty",
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
                          "field name" : "price",
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
              ]
            }
          }
        },
        {
          "field name" : "cnt",
          "field expression" : 
          {
            "iterator kind" : "FUNC_COUNT_STAR"
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "Grouping by the first expression in the SELECT list",
  "SELECT expressions" : [
    {
      "field name" : "state",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "state",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "amounts",
      "field expression" : 
      {
        "iterator kind" : "FUNC_COLLECT",
        "distinct" : true,
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "amounts",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    },
    {
      "field name" : "cnt",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "cnt",
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