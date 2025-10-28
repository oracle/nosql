compiled-query-plan

{
"query file" : "idc_inner_join/q/q21.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "order by fields at positions" : [ 0, 1, 3 ],
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
            "target table" : "company.department",
            "row variable" : "$d",
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
          "FROM variable" : "$d",
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
                  "variable" : "$d"
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
            "target table" : "company.null_records",
            "row variable" : "$nr",
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
          "FROM variable" : "$nr",
          "WHERE" : 
          {
            "iterator kind" : "AND",
            "input iterators" : [
              {
                "iterator kind" : "OP_IS_NOT_NULL",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "value",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$nr"
                  }
                }
              },
              {
                "iterator kind" : "OR",
                "input iterators" : [
                  {
                    "iterator kind" : "EQUAL",
                    "left operand" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "value",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$nr"
                      }
                    },
                    "right operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 2147483647
                    }
                  },
                  {
                    "iterator kind" : "EQUAL",
                    "left operand" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "value",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$nr"
                      }
                    },
                    "right operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : -2147483648
                    }
                  }
                ]
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
      "iterator kind" : "KEYS",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "budget_breakdown",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$d"
        }
      }
    },
    "FROM variable" : "$budget_sector",
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
            "variable" : "$d"
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
            "variable" : "$d"
          }
        }
      },
      {
        "field name" : "budget_sector",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$budget_sector"
        }
      },
      {
        "field name" : "record_id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "record_id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$nr"
          }
        }
      },
      {
        "field name" : "value",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "value",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$nr"
          }
        }
      }
    ]
  }
}
}