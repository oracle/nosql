compiled-query-plan

{
"query file" : "idc_new_timestamp/q/timestamp_floor_idx02.q",
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
      "target table" : "roundFunc",
      "row variable" : "$$roundFunc",
      "index used" : "idx_floor_t3_hour",
      "covering index" : true,
      "index row variable" : "$$roundFunc_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "timestamp_floor#t3@,'hour'" : { "start value" : "2020-02-28T23:00:00Z", "start inclusive" : false } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$roundFunc_idx",
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
            "variable" : "$$roundFunc_idx"
          }
        }
      },
      {
        "field name" : "t3_to_hour",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "timestamp_floor#t3@,'hour'",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$roundFunc_idx"
          }
        }
      }
    ]
  }
}
}