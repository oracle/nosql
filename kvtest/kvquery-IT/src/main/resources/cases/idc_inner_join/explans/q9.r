compiled-query-plan

{
"query file" : "idc_inner_join/q/q9.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0, 1 ],
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
          { "outerBranch" :0, "outerExpr" : 1, "innerVar" : 1 },
          { "outerBranch" :0, "outerExpr" : 2, "innerVar" : 2 },
          { "outerBranch" :1, "outerExpr" : 0, "innerVar" : 3 }
        ],
        "branches" : [
          {
            "iterator kind" : "SELECT",
            "FROM" :
            {
              "iterator kind" : "TABLE",
              "target table" : "company.department.team.employee",
              "row variable" : "$$emp",
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
            "FROM variable" : "$$emp",
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
                    "variable" : "$$emp"
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
                    "variable" : "$$emp"
                  }
                }
              },
              {
                "field name" : "outerJoinVal3",
                "field expression" : 
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "emp_id",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$emp"
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
              "row variable" : "$$reviewer",
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
              "position in join" : 1
            },
            "FROM variable" : "$$reviewer",
            "SELECT expressions" : [
              {
                "field name" : "outerJoinVal1",
                "field expression" : 
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "emp_id",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$reviewer"
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
              "target table" : "company.reviews",
              "row variable" : "$$r",
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
              "position in join" : 2
            },
            "FROM variable" : "$$r",
            "WHERE" : 
            {
              "iterator kind" : "AND",
              "input iterators" : [
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
                    "field name" : "emp_id",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$r"
                    }
                  }
                },
                {
                  "iterator kind" : "EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "EXTERNAL_VAR_REF",
                    "variable" : "$innerJoinVar3"
                  },
                  "right operand" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "reviewer_emp_id",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "feedback",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$r"
                      }
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
          "field name" : "company_id",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "company_id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$emp"
            }
          }
        },
        {
          "field name" : "emp_id",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "emp_id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$emp"
            }
          }
        },
        {
          "field name" : "rev_dept_id",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "department_id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$reviewer"
            }
          }
        },
        {
          "field name" : "rev_emp_id",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "emp_id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$reviewer"
            }
          }
        },
        {
          "field name" : "feedback",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "feedback",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$r"
            }
          }
        }
      ]
    }
  }
}
}