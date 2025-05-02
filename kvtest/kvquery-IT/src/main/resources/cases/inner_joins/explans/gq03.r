compiled-query-plan
{
"query file" : "inner_joins/q/gq03.q",
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
            { "outerBranch" :0, "outerExpr" : 0, "innerVar" : 0 }
          ],
          "branches" : [
            {
              "iterator kind" : "SELECT",
              "FROM" :
              {
                "iterator kind" : "TABLE",
                "target table" : "profile.messages",
                "row variable" : "$$msgs",
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
              "FROM variable" : "$$msgs",
              "SELECT expressions" : [
                {
                  "field name" : "outerJoinVal1",
                  "field expression" : 
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "uid",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$msgs"
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
                "target table" : "profile",
                "row variable" : "$$p",
                "index used" : "primary index",
                "covering index" : false,
                "index scans" : [
                  {
                    "equality conditions" : {"uid":0},
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
              "FROM variable" : "$$p",
              "WHERE" : 
              {
                "iterator kind" : "GREATER_THAN",
                "left operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "age",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$p"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 50
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
            "field name" : "uid",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "uid",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$msgs"
              }
            }
          },
          {
            "field name" : "cnt",
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
          "field name" : "uid",
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
      "field name" : "uid",
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
        "field name" : "cnt",
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
