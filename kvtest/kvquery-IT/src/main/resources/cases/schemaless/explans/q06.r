compiled-query-plan

{
"query file" : "schemaless/q/q06.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-1",
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "distinct by fields at positions" : [ 1, 2 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Viewers",
        "row variable" : "$$v",
        "index used" : "idx_country_genre",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"country":"USA","shows[].genres[]":"french"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"country":"USA","shows[].genres[]":"danish"},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$v",
      "WHERE" : 
      {
        "iterator kind" : "OP_EXISTS",
        "input iterator" :
        {
          "iterator kind" : "ARRAY_FILTER",
          "predicate iterator" :
          {
            "iterator kind" : "AND",
            "input iterators" : [
              {
                "iterator kind" : "OP_EXISTS",
                "input iterator" :
                {
                  "iterator kind" : "ARRAY_FILTER",
                  "predicate iterator" :
                  {
                    "iterator kind" : "IN",
                    "left-hand-side expressions" : [
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$element"
                      }
                    ],
                    "right-hand-side expressions" : [
                      {
                        "iterator kind" : "CONST",
                        "value" : "french"
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : "danish"
                      }
                    ]
                  },
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "genres",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$element"
                    }
                  }
                }
              },
              {
                "iterator kind" : "OP_EXISTS",
                "input iterator" :
                {
                  "iterator kind" : "ARRAY_FILTER",
                  "predicate iterator" :
                  {
                    "iterator kind" : "AND",
                    "input iterators" : [
                      {
                        "iterator kind" : "LESS_OR_EQUAL",
                        "left operand" :
                        {
                          "iterator kind" : "CONST",
                          "value" : "2021-01-01"
                        },
                        "right operand" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "date",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$element"
                          }
                        }
                      },
                      {
                        "iterator kind" : "LESS_OR_EQUAL",
                        "left operand" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "date",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$element"
                          }
                        },
                        "right operand" :
                        {
                          "iterator kind" : "CONST",
                          "value" : "2021-12-31"
                        }
                      }
                    ]
                  },
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "episodes",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "seasons",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$element"
                      }
                    }
                  }
                }
              }
            ]
          },
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "shows",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$v"
            }
          }
        }
      },
      "SELECT expressions" : [
        {
          "field name" : "cnt",
          "field expression" : 
          {
            "iterator kind" : "CONST",
            "value" : 1
          }
        },
        {
          "field name" : "acct_id_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "acct_id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$v"
            }
          }
        },
        {
          "field name" : "user_id_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "user_id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$v"
            }
          }
        }
      ]
    }
  },
  "grouping expressions" : [

  ],
  "aggregate functions" : [
    {
      "iterator kind" : "FUNC_COUNT_STAR"
    }
  ]
}
}