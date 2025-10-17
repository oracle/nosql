compiled-query-plan
{
"query file" : "idc_multirow_update/q/q8.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "UPDATE_ROW",
    "indexes to update" : [ "idxAge", "idxCode" ],
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
        "row variable" : "$$u",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"sid1":1,"sid2":2},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$u",
      "SELECT expressions" : [
        {
          "field name" : "u",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$u"
          }
        }
      ]
    }
  }
}
}
