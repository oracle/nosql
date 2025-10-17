compiled-query-plan
{
"query file" : "idc_multirow_update/q/fuzz03.q",
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
          "field name" : "ida2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$a"
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "CONST",
          "value" : 4793594583598
        }
      },
      {
        "iterator kind" : "ADD",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$a"
          }
        },
        "position iterator" :
        {
          "iterator kind" : "CONST",
          "value" : 1
        },
        "new value iterator" :
        {
          "iterator kind" : "CONST",
          "value" : "!@#$%^&*()"
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
          "field name" : "ida4",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$a"
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "CONST",
          "value" : NaN
        }
      },
      {
        "iterator kind" : "PUT",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida5",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$a"
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "new_entry"
            },
            {
              "iterator kind" : "CONST",
              "value" : NaN
            }
          ]
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
        "target table" : "A",
        "row variable" : "$$a",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"ida1":"&`b|nm./^*@!漢字 😀漢"},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$a",
      "SELECT expressions" : [
        {
          "field name" : "a",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$a"
          }
        }
      ]
    }
  }
}
}
