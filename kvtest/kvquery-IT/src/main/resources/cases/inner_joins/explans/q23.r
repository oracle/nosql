compiled-query-plan
{
"query file" : "inner_joins/q/q23.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0, 1 ],
  "input iterator" :
  {
    "iterator kind" : "GROUP",
    "input variable" : "$gb-3",
    "input iterator" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_PARTITIONS",
      "input iterator" :
      {
        "iterator kind" : "GROUP",
        "input variable" : "$gb-2",
        "input iterator" :
        {
          "iterator kind" : "SELECT",
          "FROM" :
          {
            "iterator kind" : "NESTED_LOOP_JOIN",
            "join predicates" : [
              { "outerBranch" :0, "outerExpr" : 0, "innerVar" : 0 },
              { "outerBranch" :0, "outerExpr" : 1, "innerVar" : 1 },
              { "outerBranch" :0, "outerExpr" : 2, "innerVar" : 2 }
            ],
            "branches" : [
              {
                "iterator kind" : "SELECT",
                "FROM" :
                {
                  "iterator kind" : "TABLE",
                  "target table" : "profile",
                  "row variable" : "$$u1",
                  "index used" : "primary index",
                  "covering index" : true,
                  "index scans" : [
                    {
                      "equality conditions" : {},
                      "range conditions" : {}
                    }
                  ],
                  "descendant tables" : [
                    { "table" : "profile.inbox", "row variable" : "$$inbox", "covering primary index" : true }
                  ],
                  "position in join" : 0
                },
                "FROM variables" : ["$$u1", "$$inbox"],
                "SELECT expressions" : [
                  {
                    "field name" : "outerJoinVal1",
                    "field expression" : 
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "uid",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$u1"
                      }
                    }
                  },
                  {
                    "field name" : "outerJoinVal2",
                    "field expression" : 
                    {
                      "iterator kind" : "OP_IS_NULL",
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "msgid",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$$inbox"
                        }
                      }
                    }
                  },
                  {
                    "field name" : "outerJoinVal3",
                    "field expression" : 
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "msgid",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$inbox"
                      }
                    }
                  }
                ]
              },
              {
                "iterator kind" : "SELECT",
                "FROM" :
                {
                  "iterator kind" : "TABLE",
                  "target table" : "profile",
                  "row variable" : "$$u2",
                  "index used" : "primary index",
                  "covering index" : true,
                  "index scans" : [
                    {
                      "equality conditions" : {"uid":0},
                      "range conditions" : {}
                    }
                  ],
                  "key bind expressions" : [
                    {
                      "iterator kind" : "EXTERNAL_VAR_REF",
                      "variable" : "$innerJoinVar0"
                    }
                  ],
                  "map of key bind expressions" : [
                    [ 0 ]
                  ],
                  "descendant tables" : [
                    { "table" : "profile.messages", "row variable" : "$$msg", "covering primary index" : false }
                  ],
                  "position in join" : 1
                },
                "FROM variables" : ["$$u2", "$$msg"],
                "WHERE" : 
                {
                  "iterator kind" : "OR",
                  "input iterators" : [
                    {
                      "iterator kind" : "AND",
                      "input iterators" : [
                        {
                          "iterator kind" : "EXTERNAL_VAR_REF",
                          "variable" : "$innerJoinVar1"
                        },
                        {
                          "iterator kind" : "OP_IS_NULL",
                          "input iterator" :
                          {
                            "iterator kind" : "FIELD_STEP",
                            "field name" : "msgid",
                            "input iterator" :
                            {
                              "iterator kind" : "VAR_REF",
                              "variable" : "$$msg"
                            }
                          }
                        }
                      ]
                    },
                    {
                      "iterator kind" : "EQUAL",
                      "left operand" :
                      {
                        "iterator kind" : "EXTERNAL_VAR_REF",
                        "variable" : "$innerJoinVar2"
                      },
                      "right operand" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "msgid",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$$msg"
                        }
                      }
                    }
                  ]
                },
                "SELECT expressions" : [

                ]
              }
            ]

          },
          "FROM variable" : "$join-0",
          "SELECT expressions" : [
            {
              "field name" : "u_uid",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "uid",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$u1"
                }
              }
            },
            {
              "field name" : "sender",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "sender",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "content",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$msg"
                  }
                }
              }
            },
            {
              "field name" : "cnt",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "msgid",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$msg"
                }
              }
            }
          ]
        },
        "grouping expressions" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "u_uid",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-2"
            }
          },
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "sender",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-2"
            }
          }
        ],
        "aggregate functions" : [
          {
            "iterator kind" : "FN_COUNT",
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
    },
    "grouping expressions" : [
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "u_uid",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-3"
        }
      },
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "sender",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-3"
        }
      }
    ],
    "aggregate functions" : [
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "cnt",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-3"
          }
        }
      }
    ]
  }
}
}
