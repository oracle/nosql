compiled-query-plan

{
"query file" : "idc_new_timestamp/q/timestamp_round_idx01.q",
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
      "index used" : "idx_round_t0",
      "covering index" : true,
      "index row variable" : "$$roundFunc_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "timestamp_round#t0" : { "start value" : "2021-01-01T00:00:00Z", "start inclusive" : true } }
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
        "field name" : "t0_to_day",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "timestamp_round#t0",
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