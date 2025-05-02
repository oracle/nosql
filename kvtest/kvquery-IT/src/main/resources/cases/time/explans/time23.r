compiled-query-plan

{
"query file" : "time/q/time23.q",
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
      "target table" : "bar2",
      "row variable" : "$$bar2",
      "index used" : "idx_tm",
      "covering index" : true,
      "index row variable" : "$$bar2_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "tm" : { "start value" : "2021-02-03T10:45:01Z", "start inclusive" : true } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$bar2_idx",
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
            "variable" : "$$bar2_idx"
          }
        }
      }
    ]
  }
}
}