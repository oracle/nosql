compiled-query-plan

{
"query file" : "maths/q/idx_round02.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "order by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "math_test",
      "row variable" : "$$math_test",
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
    "FROM variable" : "$$math_test",
    "WHERE" : 
    {
      "iterator kind" : "GREATER_THAN",
      "left operand" :
      {
        "iterator kind" : "ROUND",
        "input iterators" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "fc",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$math_test"
            }
          },
          {
            "iterator kind" : "CONST",
            "value" : 2
          }
        ]
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 0.0
      }
    },
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$math_test"
          }
        }
      },
      {
        "field name" : "fc",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "fc",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$math_test"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      },
      {
        "field name" : "Column_3",
        "field expression" : 
        {
          "iterator kind" : "ROUND",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "fc",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$math_test"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      }
    ]
  }
}
}