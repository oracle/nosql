compiled-query-plan

{
"query file" : "gb/q/noidx_collect_d04.q",
"plan" : 
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
          "target table" : "Bar",
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
        "SELECT expressions" : [
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
            "field name" : "amounts",
            "field expression" : 
            {
              "iterator kind" : "ARRAY_CONSTRUCTOR",
              "conditional" : true,
              "input iterators" : [
                {
                  "iterator kind" : "SEQ_MAP",
                  "mapper iterator" :
                  {
                    "iterator kind" : "CAST",
                    "target type" : "Long",
                    "quantifier" : "",
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
                    }
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
          },
          {
            "field name" : "cnt",
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
          "field name" : "year",
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
          "distinct" : true,
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "amounts",
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
      "field name" : "year",
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
      "distinct" : true,
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "amounts",
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
        "field name" : "cnt",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-2"
        }
      }
    }
  ]
}
}