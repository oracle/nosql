compiled-query-plan

{
"query file" : "idc_untyped_json_index/q/untyped04.q",
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
      "target table" : "employee.skill",
      "row variable" : "$$s",
      "index used" : "idx_skill",
      "covering index" : true,
      "index row variable" : "$$s_idx",
      "index scans" : [
        {
          "equality conditions" : {"info.skill":"java"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$s_idx",
    "SELECT expressions" : [
      {
        "field name" : "child_id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#child_id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$s_idx"
          }
        }
      }
    ]
  }
}
}