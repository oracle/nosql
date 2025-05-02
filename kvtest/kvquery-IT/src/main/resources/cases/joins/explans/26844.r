compiled-query-plan

{
"query file" : "joins/q/26844.q",
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
      "target table" : "X",
      "row variable" : "$$x",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "descendant tables" : [
        { "table" : "X.Y", "row variable" : "$$y", "covering primary index" : false }
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