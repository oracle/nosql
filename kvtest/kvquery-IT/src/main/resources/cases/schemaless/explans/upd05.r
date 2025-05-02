compiled-query-plan

{
"query file" : "schemaless/q/upd05.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "UPDATE_ROW",
    "indexes to update" : [  ],
    "update clauses" : [
      {
        "iterator kind" : "PUT",
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
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "acct_id"
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
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
            "equality conditions" : {"acct_id":100,"user_id":2},
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