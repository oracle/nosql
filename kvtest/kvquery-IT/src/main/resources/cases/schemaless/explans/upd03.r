compiled-query-plan

{
"query file" : "schemaless/q/upd03.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
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
            "field name" : "firstName",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$v"
            }
          },
          "new value iterator" :
          {
            "iterator kind" : "CONST",
            "value" : "Manolo"
          }
        },
        {
          "iterator kind" : "REMOVE",
          "clone new values" : false,
          "theIsMRCounterDec" : false,
          "theJsonMRCounterColPos" : -1,
          "theIsJsonMRCounterUpdate" : false,
          "target iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "new",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$v"
            }
          }
        },
        {
          "iterator kind" : "REMOVE",
          "clone new values" : false,
          "theIsMRCounterDec" : false,
          "theJsonMRCounterColPos" : -1,
          "theIsJsonMRCounterUpdate" : false,
          "target iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "notExists",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$v"
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
          "target table" : "Viewers",
          "row variable" : "$$v",
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