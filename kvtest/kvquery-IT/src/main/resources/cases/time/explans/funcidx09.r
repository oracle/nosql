compiled-query-plan

{
"query file" : "time/q/funcidx09.q",
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
      "index used" : "idx_year_month",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$bar",
    "SELECT expressions" : [
      {
        "field name" : "bar",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$bar"
        }
      }
    ]
  }
}
}