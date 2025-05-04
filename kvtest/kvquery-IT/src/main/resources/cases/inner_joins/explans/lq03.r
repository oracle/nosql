compiled-query-plan
{
"query file" : "inner_joins/q/lq03.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
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
            "index used" : "idx2_msgs_receivers",
            "covering index" : false,
            "index scans" : [
              {
                "equality conditions" : {"content.receivers[]":""},
                "range conditions" : {}
              }
            ],
            "key bind expressions" : [
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$innerJoinVar2"
              }
            ],
            "map of key bind expressions" : [
              [ 0 ]
            ],
            "position in join" : 1
          },
          "FROM variable" : "$$msgs1",
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
                  "variable" : "$$msgs1"
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
                "equality conditions" : {"uid":1},
                "range conditions" : {}
              }
            ],
            "index filtering predicate" :
            {
              "iterator kind" : "AND",
              "input iterators" : [
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
                {
                  "iterator kind" : "EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "EXTERNAL_VAR_REF",
                    "variable" : "$innerJoinVar1"
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
                }
              ]
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
          "field name" : "msgid",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$msgs1"
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
    ],
    "OFFSET" :
    {
      "iterator kind" : "CONST",
      "value" : 3
    },
    "LIMIT" :
    {
      "iterator kind" : "CONST",
      "value" : 11
    }
  }
}
}
