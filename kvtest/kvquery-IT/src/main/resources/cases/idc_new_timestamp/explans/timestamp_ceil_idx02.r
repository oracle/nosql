compiled-query-plan

{
"query file" : "idc_new_timestamp/q/timestamp_ceil_idx02.q",
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
      "index used" : "idx_ceil_t0_quarter",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "timestamp_ceil#t0@,'quarter'" : { "start value" : "2021-10-01T00:00:00Z", "start inclusive" : true } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$roundFunc",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$roundFunc"
          }
        }
      },
      {
        "field name" : "t0_to_quarter",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_CEIL",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "t0",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$roundFunc"
            }
          }
        }
      }
    ]
  }
}
}