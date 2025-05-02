compiled-query-plan

{
"query file" : "joins/q/lina16.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "A.B.C.D",
      "row variable" : "$$d",
      "index used" : "d_idx_d2",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "d2" : { "end value" : 10, "end inclusive" : false } }
        }
      ],
      "ancestor tables" : [
        { "table" : "A", "row variable" : "$$A", "covering primary index" : false }      ],
      "position in join" : 0
    },
    "FROM variables" : ["$$A", "$$d"],
    "SELECT expressions" : [
      {
        "field name" : "A",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$A"
        }
      },
      {
        "field name" : "d",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$d"
        }
      }
    ]
  }
}
}