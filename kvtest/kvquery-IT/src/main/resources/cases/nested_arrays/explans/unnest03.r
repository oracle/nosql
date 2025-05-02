compiled-query-plan

{
"query file" : "nested_arrays/q/unnest03.q",
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
      "row variable" : "$t",
      "index used" : "idx_areacode_number_long",
      "covering index" : true,
      "index row variable" : "$t_idx",
      "index scans" : [
        {
          "equality conditions" : {"info.addresses[].phones[][][].areacode":400},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$t_idx",
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
            "variable" : "$t_idx"
          }
        }
      }
    ]
  }
}
}