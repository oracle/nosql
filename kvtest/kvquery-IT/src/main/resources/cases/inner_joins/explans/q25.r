compiled-query-plan

{
"query file" : "inner_joins/q/q25.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 2 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "NESTED_LOOP_JOIN",
        "join predicates" : [
          { "outerBranch" :0, "outerExpr" : 0, "innerVar" : 0 }
        ],
        "branches" : [
          {
            "iterator kind" : "SELECT",
            "FROM" :
            {
              "iterator kind" : "TABLE",
              "target table" : "A",
              "row variable" : "$$a",
              "index used" : "idxS",
              "covering index" : true,
              "index row variable" : "$$a_idx",
              "index scans" : [
                {
                  "equality conditions" : {"s":"a1"},
                  "range conditions" : {}
                }
              ],
              "position in join" : 0
            },
            "FROM variable" : "$$a_idx",
            "SELECT expressions" : [
              {
                "field name" : "outerJoinVal1",
                "field expression" : 
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "#sid",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$a_idx"
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
              "target table" : "A.B",
              "row variable" : "$$b",
              "index used" : "idxS",
              "covering index" : true,
              "index row variable" : "$$b_idx",
              "index scans" : [
                {
                  "equality conditions" : {},
                  "range conditions" : {}
                }
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
                  "field name" : "#sid",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$b_idx"
                  }
                }
              },
              "position in join" : 1
            },
            "FROM variable" : "$$b_idx",
            "SELECT expressions" : [

            ]
          }
        ]

      },
      "FROM variable" : "$join-0",
      "SELECT expressions" : [
        {
          "field name" : "a",
          "field expression" : 
          {
            "iterator kind" : "RECORD_CONSTRUCTOR",
            "type" : { "Record" : {
                "sid" : "String",
                "id" : "Integer",
                "s" : "String"
              }
            },
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "#sid",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$a_idx"
                }
              },
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "#id",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$a_idx"
                }
              },
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "s",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$a_idx"
                }
              }
            ]
          }
        },
        {
          "field name" : "b",
          "field expression" : 
          {
            "iterator kind" : "RECORD_CONSTRUCTOR",
            "type" : { "Record" : {
                "sid" : "String",
                "id" : "Integer",
                "bid" : "Integer",
                "s" : "String"
              }
            },
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "#sid",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$b_idx"
                }
              },
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "#id",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$b_idx"
                }
              },
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "#bid",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$b_idx"
                }
              },
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "s",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$b_idx"
                }
              }
            ]
          }
        },
        {
          "field name" : "sort_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "s",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$a_idx"
            }
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "SELECT expressions" : [
    {
      "field name" : "a",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "a",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "b",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "b",
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