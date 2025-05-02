compiled-query-plan

{
"query file" : "gb/q/noidx_collect05.q",
"plan" : 
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
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$f",
      "GROUP BY" : "No grouping expressions",
      "SELECT expressions" : [
        {
          "field name" : "accounts",
          "field expression" : 
          {
            "iterator kind" : "FUNC_COLLECT",
            "distinct" : false,
            "input iterator" :
            {
              "iterator kind" : "MAP_CONSTRUCTOR",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : "id1"
                },
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "id1",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "id2"
                },
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "id2",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "id3"
                },
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "id3",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
                  }
                },
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
  "GROUP BY" : "No grouping expressions",
  "SELECT expressions" : [
    {
      "field name" : "accounts",
      "field expression" : 
      {
        "iterator kind" : "FUNC_COLLECT",
        "distinct" : false,
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "accounts",
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