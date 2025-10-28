compiled-query-plan
{
"query file" : "idc_inner_join/q/q13.q",
"plan" : 
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
            { "outerBranch" :1, "outerExpr" : 0, "innerVar" : 1 },
            { "outerBranch" :0, "outerExpr" : 1, "innerVar" : 2 }
          ],
          "branches" : [
            {
              "iterator kind" : "SELECT",
              "FROM" :
              {
                "iterator kind" : "TABLE",
                "target table" : "company.department.team",
                "row variable" : "$$t",
                "index used" : "primary index",
                "covering index" : true,
                "index scans" : [
                  {
                    "equality conditions" : {},
                    "range conditions" : {}
                  }
                ],
                "position in join" : 0
              },
              "FROM variable" : "$$t",
              "SELECT expressions" : [
                {
                  "field name" : "outerJoinVal1",
                  "field expression" : 
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "company_id",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$t"
                    }
                  }
                },
                {
                  "field name" : "outerJoinVal2",
                  "field expression" : 
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "team_id",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$t"
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
                "target table" : "company.department.team.employee",
                "row variable" : "$$e",
                "index used" : "primary index",
                "covering index" : false,
                "index scans" : [
                  {
                    "equality conditions" : {"company_id":0},
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
                "index filtering predicate" :
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
                    "field name" : "team_id",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$e"
                    }
                  }
                },
                "position in join" : 1
              },
              "FROM variable" : "$$e",
              "SELECT expressions" : [
                {
                  "field name" : "outerJoinVal1",
                  "field expression" : 
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "company_id",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$e"
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
                "target table" : "company.skill",
                "row variable" : "$$s",
                "index used" : "primary index",
                "covering index" : false,
                "index scans" : [
                  {
                    "equality conditions" : {"company_id":0},
                    "range conditions" : {}
                  }
                ],
                "key bind expressions" : [
                  {
                    "iterator kind" : "EXTERNAL_VAR_REF",
                    "variable" : "$innerJoinVar1"
                  }
                ],
                "map of key bind expressions" : [
                  [ 0 ]
                ],
                "position in join" : 2
              },
              "FROM variable" : "$$s",
              "SELECT expressions" : [

              ]
            }
          ]

        },
        "FROM variable" : "$join-0",
        "FROM" :
        {
          "iterator kind" : "ARRAY_FILTER",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "skills",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$e"
            }
          }
        },
        "FROM variable" : "$s",
        "WHERE" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$s"
          },
          "right operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "skill_id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$s"
            }
          }
        },
        "SELECT expressions" : [
          {
            "field name" : "company_id",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "company_id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          },
          {
            "field name" : "team_id",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "team_id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          },
          {
            "field name" : "count",
            "field expression" : 
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          },
          {
            "field name" : "total_skill",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "skill_value",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$s"
              }
            }
          },
          {
            "field name" : "min",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "skill_value",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$s"
              }
            }
          },
          {
            "field name" : "max",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "skill_value",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$s"
              }
            }
          }
        ]
      },
      "grouping expressions" : [
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "company_id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-2"
          }
        },
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "team_id",
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
        },
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "total_skill",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-2"
            }
          }
        },
        {
          "iterator kind" : "FN_MIN",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "min",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-2"
            }
          }
        },
        {
          "iterator kind" : "FN_MAX",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "max",
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
      "field name" : "company_id",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-3"
      }
    },
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "team_id",
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
        "field name" : "count",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-3"
        }
      }
    },
    {
      "iterator kind" : "FUNC_SUM",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "total_skill",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-3"
        }
      }
    },
    {
      "iterator kind" : "FN_MIN",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "min",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-3"
        }
      }
    },
    {
      "iterator kind" : "FN_MAX",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "max",
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
