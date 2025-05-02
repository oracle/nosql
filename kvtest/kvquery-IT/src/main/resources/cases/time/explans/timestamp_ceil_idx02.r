compiled-query-plan

{
"query file" : "time/q/timestamp_ceil_idx02.q",
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
      "target table" : "roundtest",
      "row variable" : "$$roundtest",
      "index used" : "idx_ceil_t0_month",
      "covering index" : true,
      "index row variable" : "$$roundtest_idx",
      "index scans" : [
        {
          "equality conditions" : {"timestamp_ceil#t0@,'month'":"2021-12-01T00:00:00Z"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$roundtest_idx",
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
            "variable" : "$$roundtest_idx"
          }
        }
      },
      {
        "field name" : "t0_to_month",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "timestamp_ceil#t0@,'month'",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$roundtest_idx"
          }
        }
      }
    ]
  }
}
}