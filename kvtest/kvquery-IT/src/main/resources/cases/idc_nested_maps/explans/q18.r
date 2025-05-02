compiled-query-plan

{
"query file" : "idc_nested_maps/q/q18.q",
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
      "target table" : "nestedTable",
      "row variable" : "$nt",
      "index used" : "idx_map1_keys_map2_values",
      "covering index" : true,
      "index row variable" : "$nt_idx",
      "index scans" : [
        {
          "equality conditions" : {"map1.keys()":"key1","map1.values().map2.values()":35},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$nt_idx",
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
            "variable" : "$nt_idx"
          }
        }
      }
    ]
  }
}
}