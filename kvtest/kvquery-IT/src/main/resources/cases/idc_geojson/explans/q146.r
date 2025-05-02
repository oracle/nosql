compiled-query-plan

{
"query file" : "idc_geojson/q/q146.q",
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
      "row variable" : "$$p",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":7},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$p",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "FN_GEO_INSIDE",
          "search target iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "geom",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$p"
              }
            }
          },
          "search geometry iterator" :
          {
            "iterator kind" : "CONST",
            "value" : {"coordinates":[[[-114.224853515625,51.12335082548444],[-114.22622680664062,51.11990293528057],[-113.91860961914062,51.273085075978415],[-114.22210693359375,51.32803736327569],[-114.50912475585936,51.104384244689136],[-114.32235717773438,50.80853820400125],[-113.82797241210938,50.85884358291],[-113.72909545898436,51.04312080348012],[-113.97491455078125,51.18622962638683],[-114.224853515625,51.12335082548444]]],"type":"Polygon"}
          }
        }
      }
    ]
  }
}
}