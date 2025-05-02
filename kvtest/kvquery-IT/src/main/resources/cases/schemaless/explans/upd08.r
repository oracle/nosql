compiled-query-plan
{
"query file" : "schemaless/q/upd08.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "UPDATE_ROW",
    "indexes to update" : [ "idx_country_genre", "idx_country_showid", "idx_country_showid_date", "idx_country_showid_seasonnum_minWatched", "idx_showid" ],
    "update clauses" : [
      {
        "iterator kind" : "JSON_MERGE_PATCH",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$v"
        },
        "new value iterator" :
        {
          "iterator kind" : "CONST",
          "value" : 3
        }
      }
    ],
    "update TTL" : false,
    "isCompletePrimaryKey" : true,
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Viewers",
        "row variable" : "$v",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"acct_id":300,"user_id":1},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$v",
      "SELECT expressions" : [
        {
          "field name" : "v",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$v"
          }
        }
      ]
    }
  }
}
}
