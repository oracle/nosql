compiled-query-plan
{
"query file" : "idc_inner_join/q/q20.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "order by fields at positions" : [ 0, 1 ],
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
            "target table" : "company.reviews",
            "row variable" : "$$r1",
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
          "FROM variable" : "$$r1",
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
                  "variable" : "$$r1"
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
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "reviewer_emp_id",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "feedback",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$r1"
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
            "target table" : "company.reviews",
            "row variable" : "$$r2",
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
          "FROM variable" : "$$r2",
          "WHERE" : 
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "ARRAY_FILTER",
              "input iterator" :
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$innerJoinVar1"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "emp_id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$r2"
              }
            }
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
            "variable" : "$$r1"
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
            "variable" : "$$r1"
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
            "variable" : "$$r1"
          }
        }
      },
      {
        "field name" : "comments",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "comments",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "feedback",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$r2"
                }
              }
            }
          ]
        }
      }
    ]
  }
}
}
