compiled-query-plan

{
"query file" : "inner_joins/q/q03.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
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
            "row variable" : "$$msgs",
            "index used" : "idx1_msgs_sender",
            "covering index" : true,
            "index row variable" : "$$msgs_idx",
            "index scans" : [
              {
                "equality conditions" : {"content.sender":"markos"},
                "range conditions" : {}
              }
            ],
            "position in join" : 0
          },
          "FROM variable" : "$$msgs_idx",
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
                  "variable" : "$$msgs_idx"
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
                  "variable" : "$$msgs_idx"
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
            "row variable" : "$$inbox",
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
          "FROM variable" : "$$inbox",
          "SELECT expressions" : [

          ]
        }
      ]

    },
    "FROM variable" : "$join-0",
    "SELECT expressions" : [
      {
        "field name" : "msgid",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#msgid",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$msgs_idx"
          }
        }
      },
      {
        "field name" : "sender",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "content.sender",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$msgs_idx"
          }
        }
      }
    ]
  }
}
}