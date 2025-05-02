compiled-query-plan

{
"query file" : "idc_geojson/q/q135.q",
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
          "equality conditions" : {"id":3},
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
            "value" : {"coordinates":[[[77.61003971099854,12.989159019587154],[77.60858058929443,12.986566336395134],[77.61514663696289,12.985876343567597],[77.61487841606139,12.98601225139729],[77.61574745178223,12.986754515924614],[77.61615514755249,12.987423597541692],[77.61617660522461,12.987737228929381],[77.61590838432312,12.988782664028546],[77.61327981948853,12.990883975267513],[77.61088728904724,12.990486713809005],[77.60996460914612,12.990058088891104],[77.61003971099854,12.989159019587154]]],"type":"Polygon"}
          }
        }
      }
    ]
  }
}
}