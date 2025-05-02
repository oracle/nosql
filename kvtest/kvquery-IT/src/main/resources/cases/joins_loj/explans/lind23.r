compiled-query-plan

{
"query file" : "joins_loj/q/lind23.q",
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
      "target table" : "M",
      "row variable" : "$$x",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "descendant tables" : [
        { "table" : "M.N", "row variable" : "$$y", "covering primary index" : false }
      ],
      "position in join" : 0
    },
    "FROM variables" : ["$$x", "$$y"],
    "SELECT expressions" : [
      {
        "field name" : "x",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$x"
        }
      },
      {
        "field name" : "y",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$y"
        }
      }
    ]
  }
}
}