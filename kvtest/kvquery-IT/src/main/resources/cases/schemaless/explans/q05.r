compiled-query-plan

{
"query file" : "schemaless/q/q05.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-1",
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "distinct by fields at positions" : [ 1, 2 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Viewers",
        "row variable" : "$$v",
        "index used" : "idx_country_showid_date",
        "covering index" : true,
        "index row variable" : "$$v_idx",
        "index scans" : [
          {
            "equality conditions" : {"country":"USA","shows[].showId":16},
            "range conditions" : { "shows[].seasons[].episodes[].date" : { "start value" : "2021-04-01", "start inclusive" : false } }
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$v_idx",
      "SELECT expressions" : [
        {
          "field name" : "cnt",
          "field expression" : 
          {
            "iterator kind" : "CONST",
            "value" : 1
          }
        },
        {
          "field name" : "acct_id_gen",
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
          "field name" : "user_id_gen",
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
  },
  "grouping expressions" : [

  ],
  "aggregate functions" : [
    {
      "iterator kind" : "FUNC_COUNT_STAR"
    }
  ]
}
}