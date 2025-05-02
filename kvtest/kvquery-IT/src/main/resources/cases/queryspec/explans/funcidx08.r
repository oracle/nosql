compiled-query-plan

{
"query file" : "queryspec/q/funcidx08.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "distinct by fields at positions" : [ 0, 1 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Viewers",
      "row variable" : "$$v",
      "index used" : "idx9_year_month",
      "covering index" : true,
      "index row variable" : "$$v_idx",
      "index scans" : [
        {
          "equality conditions" : {"substring#info.shows[].seasons[].episodes[].date@,0,4":"2021"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$v_idx",
    "SELECT expressions" : [
      {
        "field name" : "acct_id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#acct_id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$v_idx"
          }
        }
      },
      {
        "field name" : "user_id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#user_id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$v_idx"
          }
        }
      }
    ]
  }
}
}