compiled-query-plan

{
"query file" : "gb/q/collect08.q",
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
          "field name" : "collect",
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
                  "value" : "items"
                },
                {
                  "iterator kind" : "ARRAY_CONSTRUCTOR",
                  "conditional" : true,
                  "input iterators" : [
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
                  ]
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
                }
              ]
            }
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
      "field name" : "collect",
      "field expression" : 
      {
        "iterator kind" : "FUNC_COLLECT",
        "distinct" : false,
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "collect",
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