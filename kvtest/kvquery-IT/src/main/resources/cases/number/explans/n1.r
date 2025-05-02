compiled-query-plan

{
"query file" : "number/q/n1.q",
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
      "target table" : "NumTable",
      "row variable" : "$$NumTable",
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
    "FROM variable" : "$$NumTable",
    "SELECT expressions" : [
      {
        "field name" : "NumTable",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$NumTable"
        }
      }
    ]
  }
}
}