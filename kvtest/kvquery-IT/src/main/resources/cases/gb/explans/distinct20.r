compiled-query-plan

{
"query file" : "gb/q/distinct20.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-1",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "GROUP",
      "input variable" : "$gb-2",
      "input iterator" :
      {
        "iterator kind" : "RECEIVE",
        "distribution kind" : "SINGLE_PARTITION",
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
            "index row variable" : "$$f_idx",
            "index scans" : [
              {
                "equality conditions" : {"xact.year":2000},
                "range conditions" : { "xact.items[].qty" : { "start value" : 3, "start inclusive" : false } }
              }
            ],
            "index filtering predicate" :
            {
              "iterator kind" : "EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "#id1",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f_idx"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 0
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
                "iterator kind" : "CONST",
                "value" : 1
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
          "iterator kind" : "FUNC_COUNT_STAR"
        }
      ]
    },
    "FROM variable" : "$from-0",
    "SELECT expressions" : [
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
      }
    ]
  },
  "grouping expressions" : [
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "cnt",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-1"
      }
    }
  ],
  "aggregate functions" : [

  ]
}
}