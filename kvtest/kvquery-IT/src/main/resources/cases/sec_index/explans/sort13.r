compiled-query-plan

{
"query file" : "sec_index/q/sort13.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 2, 0 ],
    "input iterator" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_SHARDS",
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "T2",
          "row variable" : "$$t",
          "index used" : "idx1",
          "covering index" : true,
          "index row variable" : "$$t_idx",
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$t_idx",
        "SELECT expressions" : [
          {
            "field name" : "id",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "#id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t_idx"
              }
            }
          },
          {
            "field name" : "name",
            "field expression" : 
            {
              "iterator kind" : "CASE",
              "clauses" : [
                {
                  "when iterator" :
                  {
                    "iterator kind" : "OP_IS_NULL",
                    "input iterator" :
                    {
                      "iterator kind" : "OP_EXISTS",
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "info.name",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$$t_idx"
                        }
                      }
                    }
                  },
                  "then iterator" :
                  {
                    "iterator kind" : "CONST",
                    "value" : "NULL"
                  }
                },
                {
                  "when iterator" :
                  {
                    "iterator kind" : "OP_EXISTS",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "info.name",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$t_idx"
                      }
                    }
                  },
                  "then iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "info.name",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$t_idx"
                    }
                  }
                },
                {
                  "else iterator" :
                  {
                    "iterator kind" : "CONST",
                    "value" : "EMPTY"
                  }
                }
              ]
            }
          },
          {
            "field name" : "sort_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info.name",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t_idx"
              }
            }
          }
        ]
      }
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "id",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "id",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "name",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "name",
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