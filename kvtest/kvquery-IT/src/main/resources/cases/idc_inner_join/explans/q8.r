compiled-query-plan
{
"query file" : "idc_inner_join/q/q8.q",
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
              { "outerBranch" :0, "outerExpr" : 1, "innerVar" : 1 }
            ],
            "branches" : [
              {
                "iterator kind" : "SELECT",
                "FROM" :
                {
                  "iterator kind" : "TABLE",
                  "target table" : "company.department.team.employee",
                  "row variable" : "$$e",
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
                  },
                  {
                    "field name" : "outerJoinVal2",
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
                  "target table" : "company.department",
                  "row variable" : "$$d",
                  "index used" : "primary index",
                  "covering index" : true,
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
                  "position in join" : 1
                },
                "FROM variable" : "$$d",
                "SELECT expressions" : [

                ]
              },
              {
                "iterator kind" : "SELECT",
                "FROM" :
                {
                  "iterator kind" : "TABLE",
                  "target table" : "company.reviews",
                  "row variable" : "$$r",
                  "index used" : "primary index",
                  "covering index" : true,
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
                "FROM variable" : "$$r",
                "SELECT expressions" : [

                ]
              }
            ]

          },
          "FROM variable" : "$join-0",
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
                  "variable" : "$$e"
                }
              }
            },
            {
              "field name" : "department_id",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "department_id",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$d"
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
            "field name" : "department_id",
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
        "field name" : "department_id",
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
      }
    ]
  }
}
}
