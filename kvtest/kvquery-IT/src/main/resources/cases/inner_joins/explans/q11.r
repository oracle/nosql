compiled-query-plan

{
"query file" : "inner_joins/q/q11.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_PARTITIONS",
    "distinct by fields at positions" : [ 4, 1, 0 ],
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
            "FROM variable" : "$$msgs2",
            "WHERE" : 
            {
              "iterator kind" : "GREATER_THAN",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "date",
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
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : "2024-07-06"
              }
            },
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
                  "equality conditions" : {},
                  "range conditions" : { "content.receivers[]" : { "start value" : "", "start inclusive" : false } }
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
          "field name" : "date",
          "field expression" : 
          {
            "iterator kind" : "ARRAY_CONSTRUCTOR",
            "conditional" : true,
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "date",
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
            ]
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
        },
        {
          "field name" : "uid_gen",
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
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "SELECT expressions" : [
    {
      "field name" : "msg1",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "msg1",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "msg2",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "msg2",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "date",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "date",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
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
          "variable" : "$from-1"
        }
      }
    }
  ]
}
}