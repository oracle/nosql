compiled-query-plan

{
"query file" : "maths/q/idx_power01.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "math_test",
      "row variable" : "$$math_test",
      "index used" : "idx_power_ic",
      "covering index" : true,
      "index row variable" : "$$math_test_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "GREATER_THAN",
        "left operand" :
        {
          "iterator kind" : "POWER",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "power#ic@,2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$math_test_idx"
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
          "value" : 1000.0
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$math_test_idx",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$math_test_idx"
          }
        }
      },
      {
        "field name" : "ic",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "power#ic@,2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$math_test_idx"
          }
        }
      },
      {
        "field name" : "Column_3",
        "field expression" : 
        {
          "iterator kind" : "MULTIPLY_DIVIDE",
          "operations and operands" : [
            {
              "operation" : "*",
              "operand" :
              {
                "iterator kind" : "POWER",
                "input iterators" : [
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "power#ic@,2",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$math_test_idx"
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
              "operation" : "/",
              "operand" :
              {
                "iterator kind" : "CONST",
                "value" : 2
              }
            }
          ]
        }
      }
    ]
  }
}
}