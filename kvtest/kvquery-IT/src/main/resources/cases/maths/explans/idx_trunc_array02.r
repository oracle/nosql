compiled-query-plan

{
"query file" : "maths/q/idx_trunc_array02.q",
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
      "row variable" : "$$m",
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
    "FROM variable" : "$$m",
    "WHERE" : 
    {
      "iterator kind" : "ANY_EQUAL",
      "left operand" :
      {
        "iterator kind" : "TRUNC",
        "input iterators" : [
          {
            "iterator kind" : "PROMOTE",
            "target type" : "Any",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_FILTER",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "doubArr",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "doc",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$m"
                  }
                }
              }
            }
          }
        ]
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 123.0
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
            "variable" : "$$m"
          }
        }
      },
      {
        "field name" : "doubArr",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "doubArr",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "doc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$m"
                }
              }
            }
          ]
        }
      }
    ]
  }
}
}