compiled-query-plan

{
"query file" : "idc_inner_join/q/loj07.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0, 1, 2, 3, 4 ],
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
              "covering index" : true,
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
                  "field name" : "project_id",
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
              "target table" : "company.department.team",
              "row variable" : "$$t",
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
              "ancestor tables" : [
                { "table" : "company.department", "row variable" : "$$d", "covering primary index" : true }              ],
              "descendant tables" : [
                { "table" : "company.department.team.employee", "row variable" : "$$e", "covering primary index" : false }
              ],
              "position in join" : 1
            },
            "FROM variables" : ["$$d", "$$t", "$$e"],
            "WHERE" : 
            {
              "iterator kind" : "IN",
              "left-hand-side expressions" : [
                {
                  "iterator kind" : "EXTERNAL_VAR_REF",
                  "variable" : "$innerJoinVar1"
                }
              ],
              "right-hand-side expressions" : [
                {
                  "iterator kind" : "ARRAY_FILTER",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "projects",
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
              "variable" : "$$t"
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
        }
      ]
    }
  }
}
}