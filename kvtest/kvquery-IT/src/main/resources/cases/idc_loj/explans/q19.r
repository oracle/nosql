compiled-query-plan

{
"query file" : "idc_loj/q/q19.q",
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
      "target table" : "A.F.Z",
      "row variable" : "$$z",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "ancestor tables" : [
        { "table" : "A", "row variable" : "$$a", "covering primary index" : false }      ],
      "position in join" : 0
    },
    "FROM variables" : ["$$a", "$$z"],
    "SELECT expressions" : [
      {
        "field name" : "a",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$a"
        }
      },
      {
        "field name" : "z",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$z"
        }
      }
    ]
  }
}
}