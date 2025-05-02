compiled-query-plan

{
"query file" : "inner_joins/q/q05.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
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
            "target table" : "profile.inbox",
            "row variable" : "$$inbox",
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
          "FROM variable" : "$$inbox",
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
                  "variable" : "$$inbox"
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
                    "field name" : "date",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "content",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$inbox"
                      }
                    }
                  }
                ]
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
            "row variable" : "$$msgs",
            "index used" : "idx1_msgs_sender",
            "covering index" : false,
            "index row variable" : "$$msgs_idx",
            "index scans" : [
              {
                "equality conditions" : {"content.sender":"markos"},
                "range conditions" : {}
              }
            ],
            "index filtering predicate" :
            {
              "iterator kind" : "EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "#uid",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$msgs_idx"
                }
              },
              "right operand" :
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$innerJoinVar0"
              }
            },
            "position in join" : 1
          },
          "FROM variable" : "$$msgs",
          "WHERE" : 
          {
            "iterator kind" : "EQUAL",
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
                  "variable" : "$$msgs"
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
        "field name" : "sender",
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
              "variable" : "$$msgs"
            }
          }
        }
      }
    ]
  }
}
}