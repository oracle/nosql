compiled-query-plan

{
"query file" : "maths/q/idx_ln_array01.q",
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
        "index used" : "idx_ln_jarray",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"ln#doc.numArr[]":0.6931471805599453},
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
          "field name" : "numArr",
          "field expression" : 
          {
            "iterator kind" : "ARRAY_CONSTRUCTOR",
            "conditional" : true,
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "numArr",
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