compiled-query-plan

{
"query file" : "inner_joins/q/lq05.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "primary key bind expressions" : [
    {
      "iterator kind" : "EXTERNAL_VAR_REF",
      "variable" : "$uid"
    }
  ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "NESTED_LOOP_JOIN",
      "join predicates" : [
        { "outerBranch" :1, "outerExpr" : 0, "innerVar" : 3 },
        { "outerBranch" :0, "outerExpr" : 0, "innerVar" : 4 },
        { "outerBranch" :0, "outerExpr" : 1, "innerVar" : 5 }
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
            "covering index" : true,
            "index row variable" : "$$msgs1_idx",
            "index scans" : [
              {
                "equality conditions" : {"content.receivers[]":""},
                "range conditions" : {}
              }
            ],
            "key bind expressions" : [
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$innerJoinVar5"
              }
            ],
            "map of key bind expressions" : [
              [ 0 ]
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
                "variable" : "$uid"
              }
            ],
            "map of key bind expressions" : [
              [ 0 ]
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
                    "variable" : "$innerJoinVar3"
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
                    "variable" : "$innerJoinVar4"
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
    ],
    "OFFSET" :
    {
      "iterator kind" : "EXTERNAL_VAR_REF",
      "variable" : "$off"
    },
    "LIMIT" :
    {
      "iterator kind" : "EXTERNAL_VAR_REF",
      "variable" : "$lim"
    }
  }
}
}