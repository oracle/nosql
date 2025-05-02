compiled-query-plan

{
"query file" : "time/q/format_timestamp_err03.q",
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
      "target table" : "arithtest",
      "row variable" : "$$t",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":0},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "SELECT expressions" : [
      {
        "field name" : "d1",
        "field expression" : 
        {
          "iterator kind" : "FN_FORMAT_TIMESTAMP",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "tm9",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "MM/dd/yyyy VV"
            },
            {
              "iterator kind" : "CONST",
              "value" : "PST"
            }
          ]
        }
      }
    ]
  }
}
}