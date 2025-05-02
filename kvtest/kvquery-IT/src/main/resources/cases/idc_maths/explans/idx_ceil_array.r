compiled-query-plan

{
"query file" : "idc_maths/q/idx_ceil_array.q",
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
        "target table" : "functional_test",
        "row variable" : "$$m",
        "index used" : "idx_ceil_array",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"ceil#douArr[]":2.0},
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
          "field name" : "douArr",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "douArr",
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