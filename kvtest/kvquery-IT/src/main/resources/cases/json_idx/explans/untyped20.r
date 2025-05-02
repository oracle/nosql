compiled-query-plan

{
"query file" : "json_idx/q/untyped20.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 2 ],
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
              "variable" : "$$b_idx"
            }
          }
        },
        {
          "field name" : "state",
          "field expression" : 
          {
            "iterator kind" : "CASE",
            "clauses" : [
              {
                "when iterator" :
                {
                  "iterator kind" : "OP_NOT_EXISTS",
                  "input iterator" :
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
                "then iterator" :
                {
                  "iterator kind" : "CONST",
                  "value" : "EMPTY"
                }
              },
              {
                "when iterator" :
                {
                  "iterator kind" : "OP_IS_NULL",
                  "input iterator" :
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
                "then iterator" :
                {
                  "iterator kind" : "CONST",
                  "value" : "NULL"
                }
              },
              {
                "else iterator" :
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
                              "field name" : "info.address.state",
                              "input iterator" :
                              {
                                "iterator kind" : "VAR_REF",
                                "variable" : "$$b_idx"
                              }
                            }
                          },
                          {
                            "iterator kind" : "LESS_THAN",
                            "left operand" :
                            {
                              "iterator kind" : "FIELD_STEP",
                              "field name" : "info.address.state",
                              "input iterator" :
                              {
                                "iterator kind" : "VAR_REF",
                                "variable" : "$$b_idx"
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
                        "field name" : "info.address.state",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$$b_idx"
                        }
                      }
                    }
                  ]
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
            "field name" : "info.address.state",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b_idx"
            }
          }
        }
      ]
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
      "field name" : "state",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "state",
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