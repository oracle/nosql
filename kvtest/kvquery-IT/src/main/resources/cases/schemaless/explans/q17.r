compiled-query-plan

{
"query file" : "schemaless/q/q17.q",
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
      "target table" : "jsoncol",
      "row variable" : "$jc",
      "index used" : "idx_name",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"address.name":"rupali"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$jc",
    "SELECT expressions" : [
      {
        "field name" : "jc",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$jc"
        }
      },
      {
        "field name" : "rem_days",
        "field expression" : 
        {
          "iterator kind" : "FUNC_REMAINING_DAYS",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$jc"
          }
        }
      }
    ]
  }
}
}