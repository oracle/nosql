compiled-query-plan

{
"query file" : "idc_geojson/q/q122.q",
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
                "value" : "type"
              },
              {
                "iterator kind" : "CONST",
                "value" : "Polygons"
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
                            "value" : -114.0410578250885
                          },
                          {
                            "iterator kind" : "CONST",
                            "value" : 51.04741080535188
                          }
                        ]
                      },
                      {
                        "iterator kind" : "ARRAY_CONSTRUCTOR",
                        "conditional" : false,
                        "input iterators" : [
                          {
                            "iterator kind" : "CONST",
                            "value" : -114.04072523117065
                          },
                          {
                            "iterator kind" : "CONST",
                            "value" : 51.04645975430831
                          }
                        ]
                      },
                      {
                        "iterator kind" : "ARRAY_CONSTRUCTOR",
                        "conditional" : false,
                        "input iterators" : [
                          {
                            "iterator kind" : "CONST",
                            "value" : -114.03851509094237
                          },
                          {
                            "iterator kind" : "CONST",
                            "value" : 51.046567675976306
                          }
                        ]
                      },
                      {
                        "iterator kind" : "ARRAY_CONSTRUCTOR",
                        "conditional" : false,
                        "input iterators" : [
                          {
                            "iterator kind" : "CONST",
                            "value" : -114.03748512268066
                          },
                          {
                            "iterator kind" : "CONST",
                            "value" : 51.04708030046554
                          }
                        ]
                      },
                      {
                        "iterator kind" : "ARRAY_CONSTRUCTOR",
                        "conditional" : false,
                        "input iterators" : [
                          {
                            "iterator kind" : "CONST",
                            "value" : -114.03986692428587
                          },
                          {
                            "iterator kind" : "CONST",
                            "value" : 51.04743104026829
                          }
                        ]
                      },
                      {
                        "iterator kind" : "ARRAY_CONSTRUCTOR",
                        "conditional" : false,
                        "input iterators" : [
                          {
                            "iterator kind" : "CONST",
                            "value" : -114.0410578250885
                          },
                          {
                            "iterator kind" : "CONST",
                            "value" : 51.04741080535188
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