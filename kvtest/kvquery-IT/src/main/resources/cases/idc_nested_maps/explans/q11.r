compiled-query-plan

{
"query file" : "idc_nested_maps/q/q11.q",
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
      "index used" : "idx_keys_number_kind",
      "covering index" : true,
      "index row variable" : "$nt_idx",
      "index scans" : [
        {
          "equality conditions" : {"addresses.values().phones.values().keys()":"phone7"},
          "range conditions" : { "addresses.values().phones.values().values().number" : { "end value" : 50, "end inclusive" : false } }
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "ANY_EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "addresses.values().phones.values().values().kind",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$nt_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : "home"
        }
      },
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