compiled-query-plan

{
"query file" : "idc_new_timestamp/q/timestamp_ceil02.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "roundFunc",
      "row variable" : "$$roundFunc",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":0},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$roundFunc",
    "SELECT expressions" : [
      {
        "field name" : "timestamp_ceil_resolve_long",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_CEIL",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "l3",
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