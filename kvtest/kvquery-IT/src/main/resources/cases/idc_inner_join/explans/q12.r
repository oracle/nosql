compiled-query-plan
{
"query file" : "idc_inner_join/q/q12.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 0, 4, 2 ],
    "input iterator" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_PARTITIONS",
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
                "target table" : "company.project",
                "row variable" : "$$p",
                "index used" : "primary index",
                "covering index" : false,
                "index scans" : [
                  {
                    "equality conditions" : {},
                    "range conditions" : {}
                  }
                ],
                "position in join" : 0
              },
              "FROM variable" : "$$p",
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
                      "variable" : "$$p"
                    }
                  }
                },
                {
                  "field name" : "outerJoinVal2",
                  "field expression" : 
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "client_id",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$p"
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
                "target table" : "company.client",
                "row variable" : "$$c",
                "index used" : "primary index",
                "covering index" : false,
                "index scans" : [
                  {
                    "equality conditions" : {"company_id":0,"client_id":0},
                    "range conditions" : {}
                  }
                ],
                "key bind expressions" : [
                  {
                    "iterator kind" : "EXTERNAL_VAR_REF",
                    "variable" : "$innerJoinVar0"
                  },
                  {
                    "iterator kind" : "EXTERNAL_VAR_REF",
                    "variable" : "$innerJoinVar1"
                  }
                ],
                "map of key bind expressions" : [
                  [ 0, 1 ]
                ],
                "position in join" : 1
              },
              "FROM variable" : "$$c",
              "WHERE" : 
              {
                "iterator kind" : "IN",
                "left-hand-side expressions" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : "Email"
                  }
                ],
                "right-hand-side expressions" : [
                  {
                    "iterator kind" : "ARRAY_FILTER",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "preferred_contact_methods",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$c"
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
        "FROM" :
        {
          "iterator kind" : "VALUES",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "project_milestones",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$p"
            }
          }
        },
        "FROM variable" : "$progress",
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
                "variable" : "$$c"
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
                "variable" : "$$c"
              }
            }
          },
          {
            "field name" : "project_id",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "project_id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$p"
              }
            }
          },
          {
            "field name" : "progress",
            "field expression" : 
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$progress"
            }
          },
          {
            "field name" : "sort_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "client_id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$c"
              }
            }
          }
        ]
      }
    }
  },
  "FROM variable" : "$from-1",
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
          "variable" : "$from-1"
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
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "project_id",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "project_id",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "progress",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "progress",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    }
  ]
}
}
