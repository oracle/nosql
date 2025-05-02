compiled-query-plan

{
"query file" : "schemaless/q/q12.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Viewers",
      "row variable" : "$$viewers",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$viewers",
    "SELECT expressions" : [
      {
        "field name" : "viewers",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$viewers"
        }
      }
    ]
  }
}
}