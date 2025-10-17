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
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "power#ic@,2" : { "start value" : 1000.0, "start inclusive" : false } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$math_test",
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
        "field name" : "ic",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ic",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$math_test"
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
                    "field name" : "ic",
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