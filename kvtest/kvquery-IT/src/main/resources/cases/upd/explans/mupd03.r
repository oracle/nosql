compiled-query-plan

{
"query file" : "upd/q/mupd03.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "UPDATE_ROW",
    "indexes to update" : [ "idxCode" ],
    "update clauses" : [

    ],
    "update TTL" : true,
    "TimeUnit" : "DAYS",
    "TTL iterator" :
    {
      "iterator kind" : "CONST",
      "value" : 2
    },
    "isCompletePrimaryKey" : false,
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "users",
        "row variable" : "$$users",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"sid":0},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$users",
      "SELECT expressions" : [
        {
          "field name" : "users",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$users"
          }
        }
      ]
    }
  }
}
}