compiled-query-plan

{
"query file" : "json_idx/q/untyped04.q",
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
      "target table" : "Bar",
      "row variable" : "$$b",
      "index used" : "idx_state_city_age",
      "covering index" : true,
      "index row variable" : "$$b_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "info.address.state" : { "end value" : "WA", "end inclusive" : false } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$b_idx",
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
            "variable" : "$$b_idx"
          }
        }
      }
    ]
  }
}
}