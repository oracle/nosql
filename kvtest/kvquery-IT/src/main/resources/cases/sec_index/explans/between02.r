compiled-query-plan

{
"query file" : "sec_index/q/between02.q",
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
      "target table" : "Foo",
      "row variable" : "$$t",
      "index used" : "idx_state_city_age",
      "covering index" : true,
      "index row variable" : "$$t_idx",
      "index scans" : [
        {
          "equality conditions" : {"address.state":"MA"},
          "range conditions" : { "address.city" : { "start value" : "C", "start inclusive" : true, "end value" : "T", "end inclusive" : true } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t_idx",
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
            "variable" : "$$t_idx"
          }
        }
      }
    ]
  }
}
}