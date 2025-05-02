compiled-query-plan
{
"query file" : "inner_joins/q/uq04.q",
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
            { "outerBranch" :0, "outerExpr" : 0, "innerVar" : 0 },
            { "outerBranch" :0, "outerExpr" : 1, "innerVar" : 1 }
          ],
          "branches" : [
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
                    "equality conditions" : {},
                    "range conditions" : {}
                  }
                ],
                "position in join" : 0
              },
              "FROM variable" : "$m",
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
                      "variable" : "$m"
                    }
                  }
                },
                {
                  "field name" : "outerJoinVal2",
                  "field expression" : 
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "msgid",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$m"
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
                "target table" : "profile.inbox",
                "row variable" : "$in",
                "index used" : "primary index",
                "covering index" : true,
                "index scans" : [
                  {
                    "equality conditions" : {"uid":0,"msgid":0},
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
              "FROM variable" : "$in",
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
            "field name" : "views",
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
        "FROM variable" : "$v",
        "SELECT expressions" : [
          {
            "field name" : "v",
            "field expression" : 
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$v"
            }
          },
          {
            "field name" : "Column_2",
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
          "field name" : "v",
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
      "field name" : "v",
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
        "field name" : "Column_2",
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
