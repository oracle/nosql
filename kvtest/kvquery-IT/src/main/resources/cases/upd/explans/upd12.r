compiled-query-plan

{
"query file" : "upd/q/upd12.q",
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
          "clone new values" : true,
          "theIsMRCounterDec" : false,
          "theJsonMRCounterColPos" : -1,
          "theIsJsonMRCounterUpdate" : false,
          "target iterator" :
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
                    "variable" : "$f"
                  }
                }
              }
            }
          },
          "new value iterator" :
          {
            "iterator kind" : "ARRAY_CONSTRUCTOR",
            "conditional" : true,
            "input iterators" : [
              {
                "iterator kind" : "CASE",
                "clauses" : [
                  {
                    "when iterator" :
                    {
                      "iterator kind" : "AND",
                      "input iterators" : [
                        {
                          "iterator kind" : "IS_OF_TYPE",
                          "target types" : [
                            {
                            "type" : { "Map" : 
                              "Any"
                            },
                            "quantifier" : "",
                            "only" : false
                            }
                          ],
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$"
                          }
                        },
                        {
                          "iterator kind" : "EQUAL",
                          "left operand" :
                          {
                            "iterator kind" : "FIELD_STEP",
                            "field name" : "areacode",
                            "input iterator" :
                            {
                              "iterator kind" : "VAR_REF",
                              "variable" : "$"
                            }
                          },
                          "right operand" :
                          {
                            "iterator kind" : "CONST",
                            "value" : 415
                          }
                        },
                        {
                          "iterator kind" : "GREATER_OR_EQUAL",
                          "left operand" :
                          {
                            "iterator kind" : "FIELD_STEP",
                            "field name" : "number",
                            "input iterator" :
                            {
                              "iterator kind" : "VAR_REF",
                              "variable" : "$"
                            }
                          },
                          "right operand" :
                          {
                            "iterator kind" : "CONST",
                            "value" : 5000000
                          }
                        }
                      ]
                    },
                    "then iterator" :
                    {
                      "iterator kind" : "SEQ_CONCAT",
                      "input iterators" : [
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$"
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
                              "value" : 416
                            }
                          ]
                        }
                      ]
                    }
                  },
                  {
                    "when iterator" :
                    {
                      "iterator kind" : "IS_OF_TYPE",
                      "target types" : [
                        {
                        "type" : "String",
                        "quantifier" : "",
                        "only" : false
                        }
                      ],
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$"
                      }
                    },
                    "then iterator" :
                    {
                      "iterator kind" : "CONST",
                      "value" : "416-500-0000"
                    }
                  }
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
          "row variable" : "$f",
          "index used" : "primary index",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {"id":11},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$f",
        "SELECT expressions" : [
          {
            "field name" : "f",
            "field expression" : 
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$f"
            }
          }
        ]
      }
    },
    "FROM variable" : "$f",
    "SELECT expressions" : [
      {
        "field name" : "address",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
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
                  "variable" : "$f"
                }
              }
            }
          ]
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
            "variable" : "$f"
          }
        }
      }
    ]
  }
}
}
