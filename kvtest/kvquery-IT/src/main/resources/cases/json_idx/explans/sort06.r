compiled-query-plan

{
"query file" : "json_idx/q/sort06.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "order by fields at positions" : [ 0, 1 ],
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
          "equality conditions" : {},
          "range conditions" : { "info.address.state" : { "start value" : "MA", "start inclusive" : true } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t_idx",
    "SELECT expressions" : [
      {
        "field name" : "state",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info.address.state",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t_idx"
          }
        }
      },
      {
        "field name" : "city",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info.address.city",
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