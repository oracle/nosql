compiled-query-plan

{
"query file" : "idc_new_timestamp/q/parse_to_timestamp09.q",
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
          "equality conditions" : {"id":6},
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
          "iterator kind" : "FN_PARSE_TO_TIMESTAMP",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "Nov 26, 2021 21-50-30.999999 PST"
            },
            {
              "iterator kind" : "CONST",
              "value" : "MMM dd, yyyy HH-mm-ss.SSSSSS zzz"
            }
          ]
        }
      }
    ]
  }
}
}