compiled-query-plan
{
"query file" : "idc_multirow_update/q/q10.q",
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
        "iterator kind" : "SET",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "seqNo",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$users"
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "CONST",
          "value" : null
        }
      }
    ],
    "update TTL" : false,
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
            "equality conditions" : {"sid1":0,"sid2":1},
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
