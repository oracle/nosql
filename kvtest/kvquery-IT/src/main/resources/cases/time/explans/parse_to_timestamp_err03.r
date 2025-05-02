compiled-query-plan

{
"query file" : "time/q/parse_to_timestamp_err03.q",
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
      "covering index" : true,
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
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "FN_PARSE_TO_TIMESTAMP",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "04/16/24 IST"
            },
            {
              "iterator kind" : "CONST",
              "value" : "MM/dd/yy z"
            }
          ]
        }
      }
    ]
  }
}
}