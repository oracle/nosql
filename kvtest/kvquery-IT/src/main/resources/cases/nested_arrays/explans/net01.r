compiled-query-plan

{
"query file" : "nested_arrays/q/net01.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "netflix",
      "row variable" : "$$n",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "user_id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$n"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 1
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$n",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "user_id"
            },
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "user_id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$n"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "value"
            },
            {
              "iterator kind" : "MAP_CONSTRUCTOR",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : "contentStreamed"
                },
                {
                  "iterator kind" : "ARRAY_CONSTRUCTOR",
                  "conditional" : true,
                  "input iterators" : [
                    {
                      "iterator kind" : "SEQ_MAP",
                      "mapper iterator" :
                      {
                        "iterator kind" : "MAP_CONSTRUCTOR",
                        "input iterators" : [
                          {
                            "iterator kind" : "CONST",
                            "value" : "seriesInfo"
                          },
                          {
                            "iterator kind" : "ARRAY_CONSTRUCTOR",
                            "conditional" : true,
                            "input iterators" : [
                              {
                                "iterator kind" : "SEQ_MAP",
                                "mapper iterator" :
                                {
                                  "iterator kind" : "MAP_CONSTRUCTOR",
                                  "input iterators" : [
                                    {
                                      "iterator kind" : "CONST",
                                      "value" : "seasonNum"
                                    },
                                    {
                                      "iterator kind" : "ARRAY_CONSTRUCTOR",
                                      "conditional" : true,
                                      "input iterators" : [
                                        {
                                          "iterator kind" : "FIELD_STEP",
                                          "field name" : "seasonNum",
                                          "input iterator" :
                                          {
                                            "iterator kind" : "VAR_REF",
                                            "variable" : "$sq2"
                                          }
                                        }
                                      ]
                                    },
                                    {
                                      "iterator kind" : "CONST",
                                      "value" : "numEpisodes"
                                    },
                                    {
                                      "iterator kind" : "ARRAY_CONSTRUCTOR",
                                      "conditional" : true,
                                      "input iterators" : [
                                        {
                                          "iterator kind" : "FIELD_STEP",
                                          "field name" : "numEpisodes",
                                          "input iterator" :
                                          {
                                            "iterator kind" : "VAR_REF",
                                            "variable" : "$sq2"
                                          }
                                        }
                                      ]
                                    }
                                  ]
                                },
                                "input iterator" :
                                {
                                  "iterator kind" : "ARRAY_FILTER",
                                  "input iterator" :
                                  {
                                    "iterator kind" : "VAR_REF",
                                    "variable" : "$sq1"
                                  }
                                }
                              }
                            ]
                          }
                        ]
                      },
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "seriesInfo",
                        "input iterator" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "contentStreamed",
                          "input iterator" :
                          {
                            "iterator kind" : "FIELD_STEP",
                            "field name" : "value",
                            "input iterator" :
                            {
                              "iterator kind" : "VAR_REF",
                              "variable" : "$$n"
                            }
                          }
                        }
                      }
                    }
                  ]
                }
              ]
            }
          ]
        }
      }
    ]
  }
}
}