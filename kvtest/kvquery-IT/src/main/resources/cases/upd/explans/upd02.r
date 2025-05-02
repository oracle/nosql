compiled-query-plan

{
"query file" : "upd/q/upd02.q",
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
        "iterator kind" : "JSON_MERGE_PATCH",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
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
        },
        "new value iterator" :
        {
          "iterator kind" : "CONST",
          "value" : {"street":"somewhere"}
        }
      },
      {
        "iterator kind" : "JSON_MERGE_PATCH",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "Mark",
          "input iterator" :
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
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "CONST",
          "value" : {"hobbies":[{"football":"defence"},"hokey"],"otherParent":{"firstName":"john"}}
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
          "field name" : "number",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_FILTER",
            "predicate iterator" :
            {
              "iterator kind" : "GREATER_THAN",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "areacode",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$element"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 400
              }
            },
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
                "value" : 2
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
          "field name" : "age",
          "input iterator" :
          {
            "iterator kind" : "VALUES",
            "input iterator" :
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
            }
          }
        },
        "new value iterator" :
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
              "operation" : "*",
              "operand" :
              {
                "iterator kind" : "CONST",
                "value" : 2
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
          "field name" : "Anna",
          "input iterator" :
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
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "age"
            },
            {
              "iterator kind" : "ADD_SUBTRACT",
              "operations and operands" : [
                {
                  "operation" : "+",
                  "operand" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "age",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$"
                    }
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
            },
            {
              "iterator kind" : "CONST",
              "value" : "school"
            },
            {
              "iterator kind" : "CONST",
              "value" : "sch_2"
            },
            {
              "iterator kind" : "CONST",
              "value" : "friends"
            },
            {
              "iterator kind" : "ARRAY_CONSTRUCTOR",
              "conditional" : false,
              "input iterators" : [

              ]
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
        "target table" : "Foo",
        "row variable" : "$$f",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"id":1},
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