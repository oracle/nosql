compiled-query-plan

{
"query file" : "queryspec/q/upd03.q",
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
        "iterator kind" : "ADD",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "foo",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "seasons",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "shows",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "info",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$v"
                }
              }
            }
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "a"
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
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
        "row variable" : "$$v",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"acct_id":1,"user_id":2},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$v",
      "SELECT expressions" : [
        {
          "field name" : "v",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$v"
          }
        }
      ]
    }
  }
}
}