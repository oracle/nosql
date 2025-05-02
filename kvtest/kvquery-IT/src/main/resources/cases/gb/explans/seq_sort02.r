compiled-query-plan

{
"query file" : "gb/q/seq_sort02.q",
"plan" : 
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
        "GROUP BY" : "Grouping by the first expression in the SELECT list",
        "SELECT expressions" : [
          {
            "field name" : "gb-0",
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
            "field name" : "aggr-1",
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
                    "value" : "prodcat"
                  },
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
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : "qty"
                  },
                  {
                    "iterator kind" : "ARRAY_CONSTRUCTOR",
                    "conditional" : true,
                    "input iterators" : [
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
                    ]
                  }
                ]
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
    "FROM variable" : "$from-1",
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
            "variable" : "$from-1"
          }
        }
      },
      {
        "field name" : "aggr-1",
        "field expression" : 
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
      }
    ]
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "acctno",
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
      "field name" : "collect",
      "field expression" : 
      {
        "iterator kind" : "ARRAY_CONSTRUCTOR",
        "conditional" : false,
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