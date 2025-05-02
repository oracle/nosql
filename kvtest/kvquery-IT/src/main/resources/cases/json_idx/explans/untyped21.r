compiled-query-plan

{
"query file" : "json_idx/q/untyped21.q",
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
          "target table" : "Bar",
          "row variable" : "$$b",
          "index used" : "idx_state_city_age",
          "covering index" : true,
          "index row variable" : "$$b_idx",
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$b_idx",
        "GROUP BY" : "Grouping by the first expression in the SELECT list",
        "SELECT expressions" : [
          {
            "field name" : "gb-0",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info.address.state",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$b_idx"
              }
            }
          },
          {
            "field name" : "aggr-1",
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
      }
    ]
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "state",
      "field expression" : 
      {
        "iterator kind" : "CASE",
        "clauses" : [
          {
            "when iterator" :
            {
              "iterator kind" : "AND",
              "input iterators" : [
                {
                  "iterator kind" : "LESS_THAN",
                  "left operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 5.2
                  },
                  "right operand" :
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
                  "iterator kind" : "LESS_THAN",
                  "left operand" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "gb-0",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$from-0"
                    }
                  },
                  "right operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 5.4
                  }
                }
              ]
            },
            "then iterator" :
            {
              "iterator kind" : "CONST",
              "value" : 5.3
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
      "field name" : "count",
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
}
}