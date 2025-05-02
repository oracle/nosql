compiled-query-plan

{
"query file" : "idc_new_timestamp/q/format_timestamp09.q",
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
      "target table" : "roundFunc",
      "row variable" : "$$roundFunc",
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
    "FROM variable" : "$$roundFunc",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "FN_FORMAT_TIMESTAMP",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "2021-11-2621:50:30.999999"
            },
            {
              "iterator kind" : "CONST",
              "value" : "yyyy-MD"
            }
          ]
        }
      }
    ]
  }
}
}