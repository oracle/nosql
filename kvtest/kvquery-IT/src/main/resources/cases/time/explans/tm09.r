compiled-query-plan

{
"query file" : "time/q/tm09.q",
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
      "target table" : "bar",
      "row variable" : "$$bar",
      "index used" : "idx_tm",
      "covering index" : true,
      "index row variable" : "$$bar_idx",
      "index scans" : [
        {
          "equality conditions" : {"tm":"2021-02-03T10:45:00.234Z"},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"tm":"2021-02-03T10:45:00.999Z"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$bar_idx",
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
            "variable" : "$$bar_idx"
          }
        }
      }
    ]
  }
}
}