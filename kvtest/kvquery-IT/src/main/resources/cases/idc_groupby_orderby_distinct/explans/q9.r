compiled-query-plan

{
"query file" : "idc_groupby_orderby_distinct/q/q9.q",
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
          "target table" : "fooNew",
          "row variable" : "$$f",
          "index used" : "idx_barNew1234",
          "covering index" : true,
          "index row variable" : "$$f_idx",
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$f_idx",
        "GROUP BY" : "Grouping by the first expression in the SELECT list",
        "SELECT expressions" : [
          {
            "field name" : "gb-0",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info.bar1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f_idx"
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
                "field name" : "info.bar2",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f_idx"
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
      }
    ]
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "b",
      "field expression" : 
      {
        "iterator kind" : "CASE",
        "clauses" : [
          {
            "when iterator" :
            {
              "iterator kind" : "IS_OF_TYPE",
              "target types" : [
                {
                "type" : "Number",
                "quantifier" : "",
                "only" : false
                }
              ],
              "input iterator" :
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
            "then iterator" :
            {
              "iterator kind" : "CAST",
              "target type" : "Double",
              "quantifier" : "",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "gb-0",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$from-0"
                }
              }
            }
          },
          {
            "else iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "gb-0",
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
      "field name" : "sum",
      "field expression" : 
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "LESS_THAN",
            "left operand" :
            {
              "iterator kind" : "CONST",
              "value" : 5
            },
            "right operand" :
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
            "iterator kind" : "LESS_OR_EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "aggr-1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$from-0"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 23.3
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