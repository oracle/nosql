compiled-query-plan
{
"query file" : "inner_joins/q/dq03.q",
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
              { "table" : "profile.messages", "row variable" : "$$msgs", "covering primary index" : false }
            ],
            "position in join" : 0
          },
          "FROM variables" : ["$$p", "$$msgs"],
          "WHERE" : 
          {
            "iterator kind" : "EQUAL",
            "left operand" :
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
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : "markos"
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
                  "variable" : "$$p"
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
