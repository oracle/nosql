compiled-query-plan
{
"query file" : "inner_joins/q/uq01.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-3",
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
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
                "index used" : "idx3_msgs_size",
                "covering index" : true,
                "index row variable" : "$m_idx",
                "index scans" : [
                  {
                    "equality conditions" : {},
                    "range conditions" : {}
                  }
                ],
                "position in join" : 0
              },
              "FROM variable" : "$m_idx",
              "SELECT expressions" : [
                {
                  "field name" : "outerJoinVal1",
                  "field expression" : 
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "#uid",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$m_idx"
                    }
                  }
                },
                {
                  "field name" : "outerJoinVal2",
                  "field expression" : 
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "#msgid",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$m_idx"
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
        "SELECT expressions" : [
          {
            "field name" : "size",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "content.size",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$m_idx"
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
          "field name" : "size",
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
      "field name" : "size",
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
