compiled-query-plan
{
"query file" : "inner_joins/q/uq05.q",
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
                "target table" : "profile",
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
                    "field name" : "uid",
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
                "target table" : "profile.messages",
                "row variable" : "$m",
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
              "FROM variable" : "$m",
              "SELECT expressions" : [

              ]
            }
          ]

        },
        "FROM variable" : "$join-0",
        "FROM" :
        {
          "iterator kind" : "ARRAY_FILTER",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "receivers",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "content",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$m"
              }
            }
          }
        },
        "FROM variable" : "$r",
        "WHERE" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "userName",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$p"
            }
          },
          "right operand" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$r"
          }
        },
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
                "variable" : "$$p"
              }
            }
          },
          {
            "field name" : "receiver",
            "field expression" : 
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$r"
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
        },
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "receiver",
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
    },
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "receiver",
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
