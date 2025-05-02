compiled-query-plan

{
"query file" : "map_index/q/upd14.q",
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
          "field name" : "c1",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "expenses",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
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
        "target table" : "Boo",
        "row variable" : "$$f",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"id":2},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$f",
      "SELECT expressions" : [
        {
          "field name" : "f",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
          }
        }
      ]
    }
  }
}
}