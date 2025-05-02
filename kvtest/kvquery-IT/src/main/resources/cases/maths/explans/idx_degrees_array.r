compiled-query-plan

{
"query file" : "maths/q/idx_degrees_array.q",
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
        "index used" : "idx_degree_array",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"degrees#doubArr[]":155.74610592780184},
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
            "iterator kind" : "FIELD_STEP",
            "field name" : "doubArr",
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
}
}