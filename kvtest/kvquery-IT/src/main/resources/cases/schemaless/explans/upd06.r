compiled-query-plan

{
"query file" : "schemaless/q/upd06.q",
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
      "indexes to update" : [ "idx_index", "idx_name" ],
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
            "field name" : "name",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "address",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$j"
              }
            }
          },
          "new value iterator" :
          {
            "iterator kind" : "CONST",
            "value" : "JAIN"
          }
        },
        {
          "iterator kind" : "SET",
          "clone new values" : false,
          "theIsMRCounterDec" : false,
          "theJsonMRCounterColPos" : -1,
          "theIsJsonMRCounterUpdate" : false,
          "target iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "index",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$j"
            }
          },
          "new value iterator" :
          {
            "iterator kind" : "CONST",
            "value" : 9970001
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
          "target table" : "jsoncol",
          "row variable" : "$j",
          "index used" : "primary index",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {"majorKey1":"cc","majorKey2":"ib","minorKey":"min1"},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$j",
        "SELECT expressions" : [
          {
            "field name" : "j",
            "field expression" : 
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$j"
            }
          }
        ]
      }
    },
    "FROM variable" : "$j",
    "SELECT expressions" : [
      {
        "field name" : "j",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$j"
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "FUNC_REMAINING_DAYS",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$j"
          }
        }
      }
    ]
  }
}
}