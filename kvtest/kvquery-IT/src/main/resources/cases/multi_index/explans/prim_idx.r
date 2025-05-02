compiled-query-plan

{
"query file" : "multi_index/q/prim_idx.q",
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
      "target table" : "usage",
      "row variable" : "$$usage",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {"tenantId":"acme","tableName":"customers"},
          "range conditions" : { "startSeconds" : { "start value" : 1508259960, "start inclusive" : true } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$usage",
    "SELECT expressions" : [
      {
        "field name" : "usage",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$usage"
        }
      }
    ],
    "LIMIT" :
    {
      "iterator kind" : "CONST",
      "value" : 1
    }
  }
}
}