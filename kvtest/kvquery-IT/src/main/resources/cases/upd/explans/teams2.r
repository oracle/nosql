compiled-query-plan

{
"query file" : "upd/q/teams2.q",
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
          "iterator kind" : "ADD",
          "clone new values" : false,
          "theIsMRCounterDec" : false,
          "theJsonMRCounterColPos" : -1,
          "theIsJsonMRCounterUpdate" : false,
          "target iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "userids",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_SLICE",
              "low bound" : 1,
              "high bound" : 1,
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "teams",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "info",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            }
          },
          "position iterator" :
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$position"
          },
          "new value iterator" :
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$add"
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
          "target table" : "teams",
          "row variable" : "$$t",
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
