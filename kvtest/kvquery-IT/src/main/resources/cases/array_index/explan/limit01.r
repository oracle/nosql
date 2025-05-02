compiled-query-plan

{
"query file" : "array_index/q/limit01.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "distinct by fields at positions" : [ 0 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Foo",
        "row variable" : "$$f",
        "index used" : "idx_b",
        "covering index" : true,
        "index row variable" : "$$f_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : { "rec.b[]" : { "end value" : 0, "end inclusive" : false } }
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$f_idx",
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
              "variable" : "$$f_idx"
            }
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "$from-0",
      "field expression" : 
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$from-0"
      }
    }
  ],
  "LIMIT" :
  {
    "iterator kind" : "CONST",
    "value" : 3
  }
}
}