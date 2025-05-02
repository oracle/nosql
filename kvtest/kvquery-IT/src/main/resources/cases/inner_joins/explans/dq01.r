compiled-query-plan
{
"query file" : "inner_joins/q/dq01.q",
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
            "covering index" : false,
            "index scans" : [
              {
                "equality conditions" : {"content.sender":"markos"},
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
                "equality conditions" : {},
                "range conditions" : {}
              }
            ],
            "descendant tables" : [
              { "table" : "profile.inbox", "row variable" : "$$inbox", "covering primary index" : true }
            ],
            "position in join" : 1
          },
          "FROM variables" : ["$$p", "$$inbox"],
          "WHERE" : 
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
                    "variable" : "$$inbox"
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
                  "field name" : "msgid",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$inbox"
                  }
                }
              }
            ]
          },
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
          "field name" : "msgid",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$msgs"
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
                  "variable" : "$$msgs"
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
      }
    ]
  }
}
}
