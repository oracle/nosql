compiled-query-plan
{
"query file" : "idc_inner_join/q/q1.q",
"plan" : 
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
        { "outerBranch" :0, "outerExpr" : 0, "innerVar" : 0 }
      ],
      "branches" : [
        {
          "iterator kind" : "SELECT",
          "FROM" :
          {
            "iterator kind" : "TABLE",
            "target table" : "company",
            "row variable" : "$$c",
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
          "FROM variable" : "$$c",
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
                  "variable" : "$$c"
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
            "position in join" : 1
          },
          "FROM variable" : "$$d",
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
            "variable" : "$$d"
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
        "field name" : "name",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "name",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$d"
          }
        }
      }
    ]
  }
}
}
