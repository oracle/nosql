compiled-query-plan

{
"query file" : "idc_geojson/q/q123.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "geotypes",
      "row variable" : "$$g",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {"id":1},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$g",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$g"
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "GEO_IS_GEOMETRY",
          "input iterator" :
          {
            "iterator kind" : "MAP_CONSTRUCTOR",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : "kind"
              },
              {
                "iterator kind" : "CONST",
                "value" : "railtrack"
              },
              {
                "iterator kind" : "CONST",
                "value" : "country"
              },
              {
                "iterator kind" : "CONST",
                "value" : "india"
              },
              {
                "iterator kind" : "CONST",
                "value" : "city"
              },
              {
                "iterator kind" : "CONST",
                "value" : "bengaluru"
              },
              {
                "iterator kind" : "CONST",
                "value" : "geom"
              },
              {
                "iterator kind" : "MAP_CONSTRUCTOR",
                "input iterators" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : "type"
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : "MultiLineStrings"
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : "coordinates"
                  },
                  {
                    "iterator kind" : "ARRAY_CONSTRUCTOR",
                    "conditional" : false,
                    "input iterators" : [
                      {
                        "iterator kind" : "ARRAY_CONSTRUCTOR",
                        "conditional" : false,
                        "input iterators" : [
                          {
                            "iterator kind" : "ARRAY_CONSTRUCTOR",
                            "conditional" : false,
                            "input iterators" : [
                              {
                                "iterator kind" : "CONST",
                                "value" : 77.59585618972778
                              },
                              {
                                "iterator kind" : "CONST",
                                "value" : 12.994762468222667
                              }
                            ]
                          },
                          {
                            "iterator kind" : "ARRAY_CONSTRUCTOR",
                            "conditional" : false,
                            "input iterators" : [
                              {
                                "iterator kind" : "CONST",
                                "value" : 77.59606003761292
                              },
                              {
                                "iterator kind" : "CONST",
                                "value" : 12.994710197830848
                              }
                            ]
                          },
                          {
                            "iterator kind" : "ARRAY_CONSTRUCTOR",
                            "conditional" : false,
                            "input iterators" : [
                              {
                                "iterator kind" : "CONST",
                                "value" : 77.59631752967834
                              },
                              {
                                "iterator kind" : "CONST",
                                "value" : 12.994542932503089
                              }
                            ]
                          },
                          {
                            "iterator kind" : "ARRAY_CONSTRUCTOR",
                            "conditional" : false,
                            "input iterators" : [
                              {
                                "iterator kind" : "CONST",
                                "value" : 77.59731531143188
                              },
                              {
                                "iterator kind" : "CONST",
                                "value" : 12.994218855609887
                              }
                            ]
                          },
                          {
                            "iterator kind" : "ARRAY_CONSTRUCTOR",
                            "conditional" : false,
                            "input iterators" : [
                              {
                                "iterator kind" : "CONST",
                                "value" : 77.59783029556274
                              },
                              {
                                "iterator kind" : "CONST",
                                "value" : 12.994020227627564
                              }
                            ]
                          }
                        ]
                      },
                      {
                        "iterator kind" : "ARRAY_CONSTRUCTOR",
                        "conditional" : false,
                        "input iterators" : [
                          {
                            "iterator kind" : "ARRAY_CONSTRUCTOR",
                            "conditional" : false,
                            "input iterators" : [
                              {
                                "iterator kind" : "CONST",
                                "value" : 77.59907484054565
                              },
                              {
                                "iterator kind" : "CONST",
                                "value" : 12.993842507719203
                              }
                            ]
                          },
                          {
                            "iterator kind" : "ARRAY_CONSTRUCTOR",
                            "conditional" : false,
                            "input iterators" : [
                              {
                                "iterator kind" : "CONST",
                                "value" : 77.59933233261108
                              },
                              {
                                "iterator kind" : "CONST",
                                "value" : 12.993528884041217
                              }
                            ]
                          },
                          {
                            "iterator kind" : "ARRAY_CONSTRUCTOR",
                            "conditional" : false,
                            "input iterators" : [
                              {
                                "iterator kind" : "CONST",
                                "value" : 77.59960055351257
                              },
                              {
                                "iterator kind" : "CONST",
                                "value" : 12.993340709644286
                              }
                            ]
                          },
                          {
                            "iterator kind" : "ARRAY_CONSTRUCTOR",
                            "conditional" : false,
                            "input iterators" : [
                              {
                                "iterator kind" : "CONST",
                                "value" : 77.60051250457764
                              },
                              {
                                "iterator kind" : "CONST",
                                "value" : 12.99303753948261
                              }
                            ]
                          }
                        ]
                      }
                    ]
                  }
                ]
              }
            ]
          }
        }
      }
    ]
  }
}
}