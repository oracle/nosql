compiled-query-plan

{
"query file" : "queryspec/q/funcidx01.q",
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
      "target table" : "Users",
      "row variable" : "$$u",
      "index used" : "idx_year_month",
      "covering index" : true,
      "index row variable" : "$$u_idx",
      "index scans" : [
        {
          "equality conditions" : {"substring#address.startDate@,0,4":"2021"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$u_idx",
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
            "variable" : "$$u_idx"
          }
        }
      }
    ]
  }
}
}