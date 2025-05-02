compiled-query-plan

{
"query file" : "gb/q/jgb11.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-1",
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "distinct by fields at positions" : [ 2, 3, 4 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Bar",
        "row variable" : "$$f",
        "index used" : "idx_year_price",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"xact.year":2000},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$f",
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
          "field name" : "sales",
          "field expression" : 
          {
            "iterator kind" : "FN_SEQ_SUM",
            "input iterator" :
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
                      "field name" : "price",
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
                      "field name" : "qty",
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
          }
        },
        {
          "field name" : "id1_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        },
        {
          "field name" : "id2_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        },
        {
          "field name" : "id3_gen",
          "field expression" : 
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
      ]
    }
  },
  "grouping expressions" : [
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "acctno",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-1"
      }
    }
  ],
  "aggregate functions" : [
    {
      "iterator kind" : "FUNC_SUM",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "sales",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-1"
        }
      }
    }
  ]
}
}