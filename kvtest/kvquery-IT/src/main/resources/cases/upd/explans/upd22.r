compiled-query-plan

{
"query file" : "upd/q/upd22.q",
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
          "iterator kind" : "ARRAY_SLICE",
          "low bound" : 0,
          "high bound" : 0,
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "c2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "ARRAY_SLICE",
          "low bound" : 1,
          "high bound" : 1,
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "c2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
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
        "target table" : "T1",
        "row variable" : "$$t",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"c1":1},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$t",
      "SELECT expressions" : [
        {
          "field name" : "t",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      ]
    }
  }
}
}
