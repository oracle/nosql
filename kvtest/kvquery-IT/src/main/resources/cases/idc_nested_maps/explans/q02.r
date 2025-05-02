compiled-query-plan

{
"query file" : "idc_nested_maps/q/q02.q",
"plan" : 
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
      "target table" : "nestedTable",
      "row variable" : "$nt",
      "index used" : "idx_age_areacode_kind",
      "covering index" : true,
      "index row variable" : "$nt_idx",
      "index scans" : [
        {
          "equality conditions" : {"age":30,"addresses.values().phones.values().values().areacode":520},
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