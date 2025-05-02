compiled-query-plan

{
"query file" : "idc_new_timestamp/q/timestamp_bucket_idx01.q",
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
      "index used" : "idx_bucket_t3_7_days",
      "covering index" : true,
      "index row variable" : "$$roundFunc_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "timestamp_bucket#t3@,'7 days'" : { "start value" : "2021-01-01T00:00:00.000000000Z", "start inclusive" : true } }
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
        "field name" : "t3_to_7_days",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "timestamp_bucket#t3@,'7 days'",
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