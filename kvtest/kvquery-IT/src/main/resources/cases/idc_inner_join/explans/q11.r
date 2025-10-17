compiled-query-plan
{
"query file" : "idc_inner_join/q/q11.q",
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
              "covering index" : false,
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
                  "iterator kind" : "ARRAY_CONSTRUCTOR",
                  "conditional" : true,
                  "input iterators" : [
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
                    }
                  ]
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
                  "equality conditions" : {"company_id":0,"skill_id":0},
                  "range conditions" : {}
                }
              ],
              "key bind expressions" : [
                {
                  "iterator kind" : "EXTERNAL_VAR_REF",
                  "variable" : "$innerJoinVar0"
                },
                {
                  "iterator kind" : "ARRAY_FILTER",
                  "input iterator" :
                  {
                    "iterator kind" : "EXTERNAL_VAR_REF",
                    "variable" : "$innerJoinVar1"
                  }
                }
              ],
              "map of key bind expressions" : [
                [ 0, 1 ]
              ],
              "bind info for in3 operator" : [
                {
                  "theNumComps" : 1,
                  "thePushedComps" : [ 0 ],
                  "theIndexFieldPositions" : [ 1 ]
                 }
              ],
              "position in join" : 1
            },
            "FROM variable" : "$$s",
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
          "field name" : "emp_name",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "name",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$e"
            }
          }
        },
        {
          "field name" : "skill",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "name",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$s"
            }
          }
        }
      ]
    }
  }
}
}
