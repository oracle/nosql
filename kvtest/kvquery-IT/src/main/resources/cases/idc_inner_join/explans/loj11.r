compiled-query-plan

{
"query file" : "idc_inner_join/q/loj11.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0, 1, 2 ],
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
          { "outerBranch" :1, "outerExpr" : 0, "innerVar" : 1 },
          { "outerBranch" :0, "outerExpr" : 1, "innerVar" : 2 },
          { "outerBranch" :1, "outerExpr" : 1, "innerVar" : 3 }
        ],
        "branches" : [
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
                  "equality conditions" : {},
                  "range conditions" : {}
                }
              ],
              "position in join" : 0
            },
            "FROM variable" : "$$r",
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
                    "variable" : "$$r"
                  }
                }
              },
              {
                "field name" : "outerJoinVal2",
                "field expression" : 
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "emp_id",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$r"
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
              "target table" : "company",
              "row variable" : "$$c2",
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
              "descendant tables" : [
                { "table" : "company.department", "row variable" : "$$d", "covering primary index" : true }
              ],
              "position in join" : 1
            },
            "FROM variables" : ["$$c2", "$$d"],
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
                    "variable" : "$$c2"
                  }
                }
              },
              {
                "field name" : "outerJoinVal2",
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
              }
            ]
          },
          {
            "iterator kind" : "SELECT",
            "FROM" :
            {
              "iterator kind" : "TABLE",
              "target table" : "company",
              "row variable" : "$$c3",
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
              "descendant tables" : [
                { "table" : "company.department.team.employee", "row variable" : "$$e", "covering primary index" : true }
              ],
              "position in join" : 2
            },
            "FROM variables" : ["$$c3", "$$e"],
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
                      "variable" : "$$e"
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
                    "field name" : "department_id",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$e"
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
              "variable" : "$$r"
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
          "field name" : "emp_id",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "emp_id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$e"
            }
          }
        },
        {
          "field name" : "review_id",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "review_id",
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