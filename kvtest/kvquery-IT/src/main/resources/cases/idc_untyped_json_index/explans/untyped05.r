compiled-query-plan

{
"query file" : "idc_untyped_json_index/q/untyped05.q",
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
      "target table" : "employee",
      "row variable" : "$$nse",
      "index used" : "idx_age",
      "covering index" : true,
      "index row variable" : "$$nse_idx",
      "index scans" : [
        {
          "equality conditions" : {"info.age":25},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$nse_idx",
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
            "variable" : "$$nse_idx"
          }
        }
      }
    ]
  }
}
}