compiled-query-plan

{
"query file" : "inner_joins/q/oq06.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "order by fields at positions" : [ 0, 1, 2, 1 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "NESTED_LOOP_JOIN",
      "join predicates" : [
        { "outerBranch" :1, "outerExpr" : 0, "innerVar" : 0 },
        { "outerBranch" :0, "outerExpr" : 0, "innerVar" : 1 },
        { "outerBranch" :0, "outerExpr" : 1, "innerVar" : 2 }
      ],
      "branches" : [
        {
          "iterator kind" : "SELECT",
          "FROM" :
          {
            "iterator kind" : "TABLE",
            "target table" : "profile.messages",
            "row variable" : "$$msgs2",
            "index used" : "idx3_msgs_size",
            "covering index" : false,
            "index scans" : [
              {
                "equality conditions" : {},
                "range conditions" : { "content.size" : { "start value" : 30, "start inclusive" : true } }
              }
            ],
            "position in join" : 0
          },
          "FROM variable" : "$$msgs2",
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
                  "variable" : "$$msgs2"
                }
              }
            },
            {
              "field name" : "outerJoinVal2",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "sender",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "content",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$msgs2"
                  }
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
            "row variable" : "$$msgs1",
            "index used" : "idx1_msgs_sender",
            "covering index" : true,
            "index row variable" : "$$msgs1_idx",
            "index scans" : [
              {
                "equality conditions" : {},
                "range conditions" : { "content.sender" : { "start value" : "", "start inclusive" : true } }
              }
            ],
            "key bind expressions" : [
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$innerJoinVar2"
              }
            ],
            "map of key bind expressions" : [
              [ 0, -1 ]
            ],
            "position in join" : 1
          },
          "FROM variable" : "$$msgs1_idx",
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
                  "variable" : "$$msgs1_idx"
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
                "variable" : "$innerJoinVar1"
              }
            ],
            "map of key bind expressions" : [
              [ 0 ]
            ],
            "index filtering predicate" :
            {
              "iterator kind" : "EQUAL",
              "left operand" :
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$innerJoinVar0"
              },
              "right operand" :
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
            "position in join" : 2
          },
          "FROM variable" : "$$p",
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
          "field name" : "size",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "content",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$msgs2"
            }
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
            "variable" : "$$msgs1_idx"
          }
        }
      },
      {
        "field name" : "msg2",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "msgid",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$msgs2"
          }
        }
      },
      {
        "field name" : "msg1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#msgid",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$msgs1_idx"
          }
        }
      },
      {
        "field name" : "userName",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "userName",
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