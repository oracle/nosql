compiled-query-plan

{
"query file" : "multi_index/q/bar01.q",
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
      "target table" : "bar",
      "row variable" : "$$bar",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "pk" : { "start value" : "", "start inclusive" : true, "end value" : "ab", "end inclusive" : true } }
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$ext6"
        }
      ],
      "map of key bind expressions" : [
        [ 0, -1 ]
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$bar",
    "SELECT expressions" : [
      {
        "field name" : "pk",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "pk",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$bar"
          }
        }
      }
    ]
  }
}
}