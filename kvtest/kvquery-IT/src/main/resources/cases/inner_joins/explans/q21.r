compiled-query-plan
{
"query file" : "inner_joins/q/q21.q",
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
        { "outerBranch" :0, "outerExpr" : 0, "innerVar" : 0 },
        { "outerBranch" :0, "outerExpr" : 1, "innerVar" : 1 },
        { "outerBranch" :0, "outerExpr" : 2, "innerVar" : 2 }
      ],
      "branches" : [
        {
          "iterator kind" : "SELECT",
          "FROM" :
          {
            "iterator kind" : "TABLE",
            "target table" : "profile.messages",
            "row variable" : "$$msgs1",
            "index used" : "primary index",
            "covering index" : false,
            "index scans" : [
              {
                "equality conditions" : {"uid":1,"msgid":21},
                "range conditions" : {}
              }
            ],
            "position in join" : 0
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
            },
            {
              "field name" : "outerJoinVal2",
              "field expression" : 
              {
                "iterator kind" : "ARRAY_CONSTRUCTOR",
                "conditional" : true,
                "input iterators" : [
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "thread_id",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "content",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$msgs1"
                      }
                    }
                  }
                ]
              }
            },
            {
              "field name" : "outerJoinVal3",
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
            }
          ]
        },
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
            "index filtering predicate" :
            {
              "iterator kind" : "NOT_EQUAL",
              "left operand" :
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$innerJoinVar2"
              },
              "right operand" :
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
            "position in join" : 1
          },
          "FROM variable" : "$$msgs2",
          "WHERE" : 
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "thread_id",
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
              "iterator kind" : "ARRAY_FILTER",
              "input iterator" :
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$innerJoinVar1"
              }
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
      }
    ]
  }
}
}
