compiled-query-plan

{
"query file" : "maths/q/idx_round01.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0 ],
  "input iterator" :
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
        "index used" : "idx_round",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : { "round#fc" : { "start value" : 0.0, "start inclusive" : false } }
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
            ]
          }
        }
      ]
    }
  }
}
}