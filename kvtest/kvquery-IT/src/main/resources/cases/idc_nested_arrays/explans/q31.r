compiled-query-plan

{
"query file" : "idc_nested_arrays/q/q31.q",
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
      "row variable" : "$$nt",
      "index used" : "idx_array_foo_bar",
      "covering index" : true,
      "index row variable" : "$$nt_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "maps[].values().array[][]" : { "start value" : 3, "start inclusive" : false, "end value" : 9, "end inclusive" : false } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$nt_idx",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#Id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$nt_idx"
          }
        }
      }
    ]
  }
}
}