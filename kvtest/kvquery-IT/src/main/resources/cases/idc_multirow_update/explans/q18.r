compiled-query-plan
{
"query file" : "idc_multirow_update/q/q18.q",
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
          "field name" : "int",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "record",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "ADD_SUBTRACT",
          "operations and operands" : [
            {
              "operation" : "+",
              "operand" :
              {
                "iterator kind" : "MULTIPLY_DIVIDE",
                "operations and operands" : [
                  {
                    "operation" : "*",
                    "operand" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$"
                    }
                  },
                  {
                    "operation" : "/",
                    "operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 10
                    }
                  }
                ]
              }
            },
            {
              "operation" : "+",
              "operand" :
              {
                "iterator kind" : "CONST",
                "value" : 100
              }
            }
          ]
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
          "field name" : "areacode",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_FILTER",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "phones",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "address",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "info",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
                  }
                }
              }
            }
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "ADD_SUBTRACT",
          "operations and operands" : [
            {
              "operation" : "+",
              "operand" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$"
              }
            },
            {
              "operation" : "+",
              "operand" :
              {
                "iterator kind" : "CONST",
                "value" : 1
              }
            }
          ]
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
          "field name" : "phones",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "address",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f"
              }
            }
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "SEQ_CONCAT",
          "input iterators" : [
            {
              "iterator kind" : "MAP_CONSTRUCTOR",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : "areacode"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 570
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "number"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 51
                }
              ]
            },
            {
              "iterator kind" : "MAP_CONSTRUCTOR",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : "areacode"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 580
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "number"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 51
                }
              ]
            }
          ]
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
          "field name" : "children",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "SEQ_CONCAT",
          "input iterators" : [
            {
              "iterator kind" : "MAP_CONSTRUCTOR",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : "Rahul"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 22
                }
              ]
            },
            {
              "iterator kind" : "MAP_CONSTRUCTOR",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : "Trump"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : null
                }
              ]
            }
          ]
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
          "field name" : "lastName",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        }
      }
    ],
    "update TTL" : true,
    "TimeUnit" : "DAYS",
    "TTL iterator" :
    {
      "iterator kind" : "CONST",
      "value" : 10
    },
    "isCompletePrimaryKey" : false,
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Foo",
        "row variable" : "$$f",
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
