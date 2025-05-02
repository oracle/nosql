compiled-query-plan

{
"query file" : "geo/q/int10.q",
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
      "target table" : "points",
      "row variable" : "$$p",
      "index used" : "idx_ptn",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0vs", "start inclusive" : true, "end value" : "1b0vs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0vu", "start inclusive" : true, "end value" : "1b0vu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0yh", "start inclusive" : true, "end value" : "1b0yh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0yk", "start inclusive" : true, "end value" : "1b0yk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0ys", "start inclusive" : true, "end value" : "1b0ys", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0yu", "start inclusive" : true, "end value" : "1b0yu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0zh", "start inclusive" : true, "end value" : "1b0zh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0zk", "start inclusive" : true, "end value" : "1b0zk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0zs", "start inclusive" : true, "end value" : "1b0zs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0zu", "start inclusive" : true, "end value" : "1b0zu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2bh", "start inclusive" : true, "end value" : "1b2bh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2bk", "start inclusive" : true, "end value" : "1b2bk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2bs", "start inclusive" : true, "end value" : "1b2bs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2bu", "start inclusive" : true, "end value" : "1b2bu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2ch", "start inclusive" : true, "end value" : "1b2ch", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2ck", "start inclusive" : true, "end value" : "1b2ck", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2cs", "start inclusive" : true, "end value" : "1b2cs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2cu", "start inclusive" : true, "end value" : "1b2cu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2fh", "start inclusive" : true, "end value" : "1b2fh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2fk", "start inclusive" : true, "end value" : "1b2fk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2fs", "start inclusive" : true, "end value" : "1b2fs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2fu", "start inclusive" : true, "end value" : "1b2fu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2gh", "start inclusive" : true, "end value" : "1b2gh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2gk", "start inclusive" : true, "end value" : "1b2gk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2gs", "start inclusive" : true, "end value" : "1b2gs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2gu", "start inclusive" : true, "end value" : "1b2gu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2uh", "start inclusive" : true, "end value" : "1b2uh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2uk", "start inclusive" : true, "end value" : "1b2uk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2us", "start inclusive" : true, "end value" : "1b2us", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2uu", "start inclusive" : true, "end value" : "1b2uu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2vh", "start inclusive" : true, "end value" : "1b2vh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2vk", "start inclusive" : true, "end value" : "1b2vk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2vs", "start inclusive" : true, "end value" : "1b2vs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2vu", "start inclusive" : true, "end value" : "1b2vu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2yh", "start inclusive" : true, "end value" : "1b2yh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2yk", "start inclusive" : true, "end value" : "1b2yk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2ys", "start inclusive" : true, "end value" : "1b2ys", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2yu", "start inclusive" : true, "end value" : "1b2yu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2zh", "start inclusive" : true, "end value" : "1b2zh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2zk", "start inclusive" : true, "end value" : "1b2zk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2zs", "start inclusive" : true, "end value" : "1b2zs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b2zu", "start inclusive" : true, "end value" : "1b2zu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8bh", "start inclusive" : true, "end value" : "1b8bh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8bk", "start inclusive" : true, "end value" : "1b8bk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8bs", "start inclusive" : true, "end value" : "1b8bs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8bu", "start inclusive" : true, "end value" : "1b8bu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8ch", "start inclusive" : true, "end value" : "1b8ch", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8ck", "start inclusive" : true, "end value" : "1b8ck", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8cs", "start inclusive" : true, "end value" : "1b8cs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8cu", "start inclusive" : true, "end value" : "1b8cu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8fh", "start inclusive" : true, "end value" : "1b8fh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8fk", "start inclusive" : true, "end value" : "1b8fk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8fs", "start inclusive" : true, "end value" : "1b8fs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8fu", "start inclusive" : true, "end value" : "1b8fu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8gh", "start inclusive" : true, "end value" : "1b8gh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8gk", "start inclusive" : true, "end value" : "1b8gk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8gs", "start inclusive" : true, "end value" : "1b8gs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8gu", "start inclusive" : true, "end value" : "1b8gu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8uh", "start inclusive" : true, "end value" : "1b8uh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8uk", "start inclusive" : true, "end value" : "1b8uk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8us", "start inclusive" : true, "end value" : "1b8us", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8uu", "start inclusive" : true, "end value" : "1b8uu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8vh", "start inclusive" : true, "end value" : "1b8vh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8vk", "start inclusive" : true, "end value" : "1b8vk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8vs", "start inclusive" : true, "end value" : "1b8vs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8vu", "start inclusive" : true, "end value" : "1b8vu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8yh", "start inclusive" : true, "end value" : "1b8yh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8yk", "start inclusive" : true, "end value" : "1b8yk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8ys", "start inclusive" : true, "end value" : "1b8ys", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8yu", "start inclusive" : true, "end value" : "1b8yu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8zh", "start inclusive" : true, "end value" : "1b8zh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8zk", "start inclusive" : true, "end value" : "1b8zk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8zs", "start inclusive" : true, "end value" : "1b8zs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b8zu", "start inclusive" : true, "end value" : "1b8zu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbbh", "start inclusive" : true, "end value" : "1bbbh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbbk", "start inclusive" : true, "end value" : "1bbbk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbbs", "start inclusive" : true, "end value" : "1bbbs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbbu", "start inclusive" : true, "end value" : "1bbbu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbch", "start inclusive" : true, "end value" : "1bbch", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbck", "start inclusive" : true, "end value" : "1bbck", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbcs", "start inclusive" : true, "end value" : "1bbcs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbcu", "start inclusive" : true, "end value" : "1bbcu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbfh", "start inclusive" : true, "end value" : "1bbfh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbfk", "start inclusive" : true, "end value" : "1bbfk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbfs", "start inclusive" : true, "end value" : "1bbfs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbfu", "start inclusive" : true, "end value" : "1bbfu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbgh", "start inclusive" : true, "end value" : "1bbgh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbgk", "start inclusive" : true, "end value" : "1bbgk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbgs", "start inclusive" : true, "end value" : "1bbgs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbgu", "start inclusive" : true, "end value" : "1bbgu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbuh", "start inclusive" : true, "end value" : "1bbuh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbuk", "start inclusive" : true, "end value" : "1bbuk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbus", "start inclusive" : true, "end value" : "1bbus", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbuu", "start inclusive" : true, "end value" : "1bbuu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbvh", "start inclusive" : true, "end value" : "1bbvh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbvk", "start inclusive" : true, "end value" : "1bbvk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbvs", "start inclusive" : true, "end value" : "1bbvs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbvu", "start inclusive" : true, "end value" : "1bbvu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbyh", "start inclusive" : true, "end value" : "1bbyh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbyk", "start inclusive" : true, "end value" : "1bbyk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbys", "start inclusive" : true, "end value" : "1bbys", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbyu", "start inclusive" : true, "end value" : "1bbyu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbzh", "start inclusive" : true, "end value" : "1bbzh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbzk", "start inclusive" : true, "end value" : "1bbzk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbzs", "start inclusive" : true, "end value" : "1bbzs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1bbzu", "start inclusive" : true, "end value" : "1bbzu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0bh", "start inclusive" : true, "end value" : "1c0bh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0bk", "start inclusive" : true, "end value" : "1c0bk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0bs", "start inclusive" : true, "end value" : "1c0bs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0bu", "start inclusive" : true, "end value" : "1c0bu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0ch", "start inclusive" : true, "end value" : "1c0ch", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0ck", "start inclusive" : true, "end value" : "1c0ck", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0cs", "start inclusive" : true, "end value" : "1c0cs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0cu", "start inclusive" : true, "end value" : "1c0cu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0fh", "start inclusive" : true, "end value" : "1c0fh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0fk", "start inclusive" : true, "end value" : "1c0fk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0fs", "start inclusive" : true, "end value" : "1c0fs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0fu", "start inclusive" : true, "end value" : "1c0fu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0gh", "start inclusive" : true, "end value" : "1c0gh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0gk", "start inclusive" : true, "end value" : "1c0gk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0gs", "start inclusive" : true, "end value" : "1c0gs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0gu", "start inclusive" : true, "end value" : "1c0gu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0uh", "start inclusive" : true, "end value" : "1c0uh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0uk", "start inclusive" : true, "end value" : "1c0uk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0us", "start inclusive" : true, "end value" : "1c0us", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0uu", "start inclusive" : true, "end value" : "1c0uu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0vh", "start inclusive" : true, "end value" : "1c0vh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0vk", "start inclusive" : true, "end value" : "1c0vk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0vs", "start inclusive" : true, "end value" : "1c0vs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0vu", "start inclusive" : true, "end value" : "1c0vu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0yh", "start inclusive" : true, "end value" : "1c0yh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0yk", "start inclusive" : true, "end value" : "1c0yk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0ys", "start inclusive" : true, "end value" : "1c0ys", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0yu", "start inclusive" : true, "end value" : "1c0yu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0zh", "start inclusive" : true, "end value" : "1c0zh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0zk", "start inclusive" : true, "end value" : "1c0zk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0zs", "start inclusive" : true, "end value" : "1c0zs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c0zu", "start inclusive" : true, "end value" : "1c0zu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2bh", "start inclusive" : true, "end value" : "1c2bh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2bk", "start inclusive" : true, "end value" : "1c2bk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2bs", "start inclusive" : true, "end value" : "1c2bs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2bu", "start inclusive" : true, "end value" : "1c2bu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2ch", "start inclusive" : true, "end value" : "1c2ch", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2ck", "start inclusive" : true, "end value" : "1c2ck", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2cs", "start inclusive" : true, "end value" : "1c2cs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2cu", "start inclusive" : true, "end value" : "1c2cu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2fh", "start inclusive" : true, "end value" : "1c2fh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2fk", "start inclusive" : true, "end value" : "1c2fk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2fs", "start inclusive" : true, "end value" : "1c2fs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2fu", "start inclusive" : true, "end value" : "1c2fu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2gh", "start inclusive" : true, "end value" : "1c2gh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2gk", "start inclusive" : true, "end value" : "1c2gk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2gs", "start inclusive" : true, "end value" : "1c2gs", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2gu", "start inclusive" : true, "end value" : "1c2gu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2uh", "start inclusive" : true, "end value" : "1c2uh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2uk", "start inclusive" : true, "end value" : "1c2uk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2us", "start inclusive" : true, "end value" : "1c2us", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2uu", "start inclusive" : true, "end value" : "1c2uu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2vh", "start inclusive" : true, "end value" : "1c2vh", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2vk", "start inclusive" : true, "end value" : "1c2vk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1c2vs", "start inclusive" : true, "end value" : "1c2vs", "end inclusive" : true } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$p",
    "WHERE" : 
    {
      "iterator kind" : "FN_GEO_INTERSECT",
      "search target iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "point",
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
        "value" : {"coordinates":[[-100,-89],[-100,-82.0]],"type":"LineString"}
      }
    },
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$p"
          }
        }
      },
      {
        "field name" : "point",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "point",
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
            }
          ]
        }
      }
    ]
  }
}
}