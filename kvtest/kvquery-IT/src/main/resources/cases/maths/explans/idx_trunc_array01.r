compiled-query-plan

{
"query file" : "maths/q/idx_trunc_array01.q",
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
        "row variable" : "$$m",
        "index used" : "idx_trunc_jarray",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"trunc#doc.doubArr[]":123.0},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$m",
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
}