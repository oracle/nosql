compiled-query-plan

{
"query file" : "geo/q/int11.q",
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
          "range conditions" : { "info.point" : { "start value" : "1b0fk2y4", "start inclusive" : true, "end value" : "1b0fk2y5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk2yh", "start inclusive" : true, "end value" : "1b0fk2yjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk2yn", "start inclusive" : true, "end value" : "1b0fk2ypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3n0", "start inclusive" : true, "end value" : "1b0fk3n1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3n4", "start inclusive" : true, "end value" : "1b0fk3n5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3nh", "start inclusive" : true, "end value" : "1b0fk3njzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3nn", "start inclusive" : true, "end value" : "1b0fk3npzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3q0", "start inclusive" : true, "end value" : "1b0fk3q1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3q4", "start inclusive" : true, "end value" : "1b0fk3q5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3qh", "start inclusive" : true, "end value" : "1b0fk3qjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3qn", "start inclusive" : true, "end value" : "1b0fk3qpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3w0", "start inclusive" : true, "end value" : "1b0fk3w1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3w4", "start inclusive" : true, "end value" : "1b0fk3w5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3wh", "start inclusive" : true, "end value" : "1b0fk3wjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3wn", "start inclusive" : true, "end value" : "1b0fk3wpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3y0", "start inclusive" : true, "end value" : "1b0fk3y1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3y4", "start inclusive" : true, "end value" : "1b0fk3y5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3yh", "start inclusive" : true, "end value" : "1b0fk3yjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk3yn", "start inclusive" : true, "end value" : "1b0fk3ypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6n0", "start inclusive" : true, "end value" : "1b0fk6n1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6n4", "start inclusive" : true, "end value" : "1b0fk6n5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6nh", "start inclusive" : true, "end value" : "1b0fk6njzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6nn", "start inclusive" : true, "end value" : "1b0fk6npzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6q0", "start inclusive" : true, "end value" : "1b0fk6q1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6q4", "start inclusive" : true, "end value" : "1b0fk6q5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6qh", "start inclusive" : true, "end value" : "1b0fk6qjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6qn", "start inclusive" : true, "end value" : "1b0fk6qpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6w0", "start inclusive" : true, "end value" : "1b0fk6w1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6w4", "start inclusive" : true, "end value" : "1b0fk6w5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6wh", "start inclusive" : true, "end value" : "1b0fk6wjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6wn", "start inclusive" : true, "end value" : "1b0fk6wpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6y0", "start inclusive" : true, "end value" : "1b0fk6y1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6y4", "start inclusive" : true, "end value" : "1b0fk6y5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6yh", "start inclusive" : true, "end value" : "1b0fk6yjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk6yn", "start inclusive" : true, "end value" : "1b0fk6ypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7n0", "start inclusive" : true, "end value" : "1b0fk7n1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7n4", "start inclusive" : true, "end value" : "1b0fk7n5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7nh", "start inclusive" : true, "end value" : "1b0fk7njzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7nn", "start inclusive" : true, "end value" : "1b0fk7npzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7q0", "start inclusive" : true, "end value" : "1b0fk7q1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7q4", "start inclusive" : true, "end value" : "1b0fk7q5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7qh", "start inclusive" : true, "end value" : "1b0fk7qjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7qn", "start inclusive" : true, "end value" : "1b0fk7qpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7w0", "start inclusive" : true, "end value" : "1b0fk7w1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7w4", "start inclusive" : true, "end value" : "1b0fk7w5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7wh", "start inclusive" : true, "end value" : "1b0fk7wjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7wn", "start inclusive" : true, "end value" : "1b0fk7wpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7y0", "start inclusive" : true, "end value" : "1b0fk7y1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7y4", "start inclusive" : true, "end value" : "1b0fk7y5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7yh", "start inclusive" : true, "end value" : "1b0fk7yjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fk7yn", "start inclusive" : true, "end value" : "1b0fk7ypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkkn0", "start inclusive" : true, "end value" : "1b0fkkn1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkkn4", "start inclusive" : true, "end value" : "1b0fkkn5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkknh", "start inclusive" : true, "end value" : "1b0fkknjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkknn", "start inclusive" : true, "end value" : "1b0fkknpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkkq0", "start inclusive" : true, "end value" : "1b0fkkq1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkkq4", "start inclusive" : true, "end value" : "1b0fkkq5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkkqh", "start inclusive" : true, "end value" : "1b0fkkqjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkkqn", "start inclusive" : true, "end value" : "1b0fkkqpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkkw0", "start inclusive" : true, "end value" : "1b0fkkw1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkkw4", "start inclusive" : true, "end value" : "1b0fkkw5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkkwh", "start inclusive" : true, "end value" : "1b0fkkwjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkkwn", "start inclusive" : true, "end value" : "1b0fkkwpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkky0", "start inclusive" : true, "end value" : "1b0fkky1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkky4", "start inclusive" : true, "end value" : "1b0fkky5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkkyh", "start inclusive" : true, "end value" : "1b0fkkyjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkkyn", "start inclusive" : true, "end value" : "1b0fkkypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmn0", "start inclusive" : true, "end value" : "1b0fkmn1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmn4", "start inclusive" : true, "end value" : "1b0fkmn5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmnh", "start inclusive" : true, "end value" : "1b0fkmnjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmnn", "start inclusive" : true, "end value" : "1b0fkmnpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmq0", "start inclusive" : true, "end value" : "1b0fkmq1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmq4", "start inclusive" : true, "end value" : "1b0fkmq5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmqh", "start inclusive" : true, "end value" : "1b0fkmqjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmqn", "start inclusive" : true, "end value" : "1b0fkmqpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmw0", "start inclusive" : true, "end value" : "1b0fkmw1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmw4", "start inclusive" : true, "end value" : "1b0fkmw5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmwh", "start inclusive" : true, "end value" : "1b0fkmwjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmwn", "start inclusive" : true, "end value" : "1b0fkmwpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmy0", "start inclusive" : true, "end value" : "1b0fkmy1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmy4", "start inclusive" : true, "end value" : "1b0fkmy5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmyh", "start inclusive" : true, "end value" : "1b0fkmyjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkmyn", "start inclusive" : true, "end value" : "1b0fkmypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqn0", "start inclusive" : true, "end value" : "1b0fkqn1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqn4", "start inclusive" : true, "end value" : "1b0fkqn5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqnh", "start inclusive" : true, "end value" : "1b0fkqnjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqnn", "start inclusive" : true, "end value" : "1b0fkqnpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqq0", "start inclusive" : true, "end value" : "1b0fkqq1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqq4", "start inclusive" : true, "end value" : "1b0fkqq5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqqh", "start inclusive" : true, "end value" : "1b0fkqqjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqqn", "start inclusive" : true, "end value" : "1b0fkqqpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqw0", "start inclusive" : true, "end value" : "1b0fkqw1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqw4", "start inclusive" : true, "end value" : "1b0fkqw5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqwh", "start inclusive" : true, "end value" : "1b0fkqwjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqwn", "start inclusive" : true, "end value" : "1b0fkqwpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqy0", "start inclusive" : true, "end value" : "1b0fkqy1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqy4", "start inclusive" : true, "end value" : "1b0fkqy5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqyh", "start inclusive" : true, "end value" : "1b0fkqyjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkqyn", "start inclusive" : true, "end value" : "1b0fkqypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkrn0", "start inclusive" : true, "end value" : "1b0fkrn1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkrn4", "start inclusive" : true, "end value" : "1b0fkrn5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkrnh", "start inclusive" : true, "end value" : "1b0fkrnjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkrnn", "start inclusive" : true, "end value" : "1b0fkrnpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkrq0", "start inclusive" : true, "end value" : "1b0fkrq1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkrq4", "start inclusive" : true, "end value" : "1b0fkrq5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkrqh", "start inclusive" : true, "end value" : "1b0fkrqjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkrqn", "start inclusive" : true, "end value" : "1b0fkrqpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkrw0", "start inclusive" : true, "end value" : "1b0fkrw1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkrw4", "start inclusive" : true, "end value" : "1b0fkrw5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkrwh", "start inclusive" : true, "end value" : "1b0fkrwjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkrwn", "start inclusive" : true, "end value" : "1b0fkrwpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkry0", "start inclusive" : true, "end value" : "1b0fkry1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkry4", "start inclusive" : true, "end value" : "1b0fkry5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkryh", "start inclusive" : true, "end value" : "1b0fkryjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fkryn", "start inclusive" : true, "end value" : "1b0fkrypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2n0", "start inclusive" : true, "end value" : "1b0fs2n1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2n4", "start inclusive" : true, "end value" : "1b0fs2n5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2nh", "start inclusive" : true, "end value" : "1b0fs2njzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2nn", "start inclusive" : true, "end value" : "1b0fs2npzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2q0", "start inclusive" : true, "end value" : "1b0fs2q1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2q4", "start inclusive" : true, "end value" : "1b0fs2q5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2qh", "start inclusive" : true, "end value" : "1b0fs2qjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2qn", "start inclusive" : true, "end value" : "1b0fs2qpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2w0", "start inclusive" : true, "end value" : "1b0fs2w1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2w4", "start inclusive" : true, "end value" : "1b0fs2w5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2wh", "start inclusive" : true, "end value" : "1b0fs2wjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2wn", "start inclusive" : true, "end value" : "1b0fs2wpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2y0", "start inclusive" : true, "end value" : "1b0fs2y1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2y4", "start inclusive" : true, "end value" : "1b0fs2y5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2yh", "start inclusive" : true, "end value" : "1b0fs2yjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs2yn", "start inclusive" : true, "end value" : "1b0fs2ypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3n0", "start inclusive" : true, "end value" : "1b0fs3n1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3n4", "start inclusive" : true, "end value" : "1b0fs3n5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3nh", "start inclusive" : true, "end value" : "1b0fs3njzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3nn", "start inclusive" : true, "end value" : "1b0fs3npzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3q0", "start inclusive" : true, "end value" : "1b0fs3q1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3q4", "start inclusive" : true, "end value" : "1b0fs3q5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3qh", "start inclusive" : true, "end value" : "1b0fs3qjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3qn", "start inclusive" : true, "end value" : "1b0fs3qpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3w0", "start inclusive" : true, "end value" : "1b0fs3w1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3w4", "start inclusive" : true, "end value" : "1b0fs3w5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3wh", "start inclusive" : true, "end value" : "1b0fs3wjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3wn", "start inclusive" : true, "end value" : "1b0fs3wpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3y0", "start inclusive" : true, "end value" : "1b0fs3y1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3y4", "start inclusive" : true, "end value" : "1b0fs3y5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3yh", "start inclusive" : true, "end value" : "1b0fs3yjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs3yn", "start inclusive" : true, "end value" : "1b0fs3ypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6n0", "start inclusive" : true, "end value" : "1b0fs6n1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6n4", "start inclusive" : true, "end value" : "1b0fs6n5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6nh", "start inclusive" : true, "end value" : "1b0fs6njzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6nn", "start inclusive" : true, "end value" : "1b0fs6npzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6q0", "start inclusive" : true, "end value" : "1b0fs6q1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6q4", "start inclusive" : true, "end value" : "1b0fs6q5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6qh", "start inclusive" : true, "end value" : "1b0fs6qjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6qn", "start inclusive" : true, "end value" : "1b0fs6qpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6w0", "start inclusive" : true, "end value" : "1b0fs6w1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6w4", "start inclusive" : true, "end value" : "1b0fs6w5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6wh", "start inclusive" : true, "end value" : "1b0fs6wjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6wn", "start inclusive" : true, "end value" : "1b0fs6wpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6y0", "start inclusive" : true, "end value" : "1b0fs6y1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6y4", "start inclusive" : true, "end value" : "1b0fs6y5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6yh", "start inclusive" : true, "end value" : "1b0fs6yjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs6yn", "start inclusive" : true, "end value" : "1b0fs6ypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7n0", "start inclusive" : true, "end value" : "1b0fs7n1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7n4", "start inclusive" : true, "end value" : "1b0fs7n5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7nh", "start inclusive" : true, "end value" : "1b0fs7njzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7nn", "start inclusive" : true, "end value" : "1b0fs7npzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7q0", "start inclusive" : true, "end value" : "1b0fs7q1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7q4", "start inclusive" : true, "end value" : "1b0fs7q5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7qh", "start inclusive" : true, "end value" : "1b0fs7qjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7qn", "start inclusive" : true, "end value" : "1b0fs7qpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7w0", "start inclusive" : true, "end value" : "1b0fs7w1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7w4", "start inclusive" : true, "end value" : "1b0fs7w5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7wh", "start inclusive" : true, "end value" : "1b0fs7wjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7wn", "start inclusive" : true, "end value" : "1b0fs7wpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7y0", "start inclusive" : true, "end value" : "1b0fs7y1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7y4", "start inclusive" : true, "end value" : "1b0fs7y5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7yh", "start inclusive" : true, "end value" : "1b0fs7yjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fs7yn", "start inclusive" : true, "end value" : "1b0fs7ypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fskn0", "start inclusive" : true, "end value" : "1b0fskn1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fskn4", "start inclusive" : true, "end value" : "1b0fskn5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsknh", "start inclusive" : true, "end value" : "1b0fsknjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsknn", "start inclusive" : true, "end value" : "1b0fsknpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fskq0", "start inclusive" : true, "end value" : "1b0fskq1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fskq4", "start inclusive" : true, "end value" : "1b0fskq5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fskqh", "start inclusive" : true, "end value" : "1b0fskqjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fskqn", "start inclusive" : true, "end value" : "1b0fskqpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fskw0", "start inclusive" : true, "end value" : "1b0fskw1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fskw4", "start inclusive" : true, "end value" : "1b0fskw5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fskwh", "start inclusive" : true, "end value" : "1b0fskwjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fskwn", "start inclusive" : true, "end value" : "1b0fskwpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsky0", "start inclusive" : true, "end value" : "1b0fsky1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsky4", "start inclusive" : true, "end value" : "1b0fsky5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fskyh", "start inclusive" : true, "end value" : "1b0fskyjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fskyn", "start inclusive" : true, "end value" : "1b0fskypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmn0", "start inclusive" : true, "end value" : "1b0fsmn1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmn4", "start inclusive" : true, "end value" : "1b0fsmn5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmnh", "start inclusive" : true, "end value" : "1b0fsmnjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmnn", "start inclusive" : true, "end value" : "1b0fsmnpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmq0", "start inclusive" : true, "end value" : "1b0fsmq1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmq4", "start inclusive" : true, "end value" : "1b0fsmq5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmqh", "start inclusive" : true, "end value" : "1b0fsmqjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmqn", "start inclusive" : true, "end value" : "1b0fsmqpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmw0", "start inclusive" : true, "end value" : "1b0fsmw1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmw4", "start inclusive" : true, "end value" : "1b0fsmw5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmwh", "start inclusive" : true, "end value" : "1b0fsmwjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmwn", "start inclusive" : true, "end value" : "1b0fsmwpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmy0", "start inclusive" : true, "end value" : "1b0fsmy1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmy4", "start inclusive" : true, "end value" : "1b0fsmy5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmyh", "start inclusive" : true, "end value" : "1b0fsmyjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsmyn", "start inclusive" : true, "end value" : "1b0fsmypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqn0", "start inclusive" : true, "end value" : "1b0fsqn1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqn4", "start inclusive" : true, "end value" : "1b0fsqn5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqnh", "start inclusive" : true, "end value" : "1b0fsqnjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqnn", "start inclusive" : true, "end value" : "1b0fsqnpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqq0", "start inclusive" : true, "end value" : "1b0fsqq1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqq4", "start inclusive" : true, "end value" : "1b0fsqq5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqqh", "start inclusive" : true, "end value" : "1b0fsqqjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqqn", "start inclusive" : true, "end value" : "1b0fsqqpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqw0", "start inclusive" : true, "end value" : "1b0fsqw1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqw4", "start inclusive" : true, "end value" : "1b0fsqw5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqwh", "start inclusive" : true, "end value" : "1b0fsqwjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqwn", "start inclusive" : true, "end value" : "1b0fsqwpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqy0", "start inclusive" : true, "end value" : "1b0fsqy1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqy4", "start inclusive" : true, "end value" : "1b0fsqy5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqyh", "start inclusive" : true, "end value" : "1b0fsqyjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsqyn", "start inclusive" : true, "end value" : "1b0fsqypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsrn0", "start inclusive" : true, "end value" : "1b0fsrn1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsrn4", "start inclusive" : true, "end value" : "1b0fsrn5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsrnh", "start inclusive" : true, "end value" : "1b0fsrnjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsrnn", "start inclusive" : true, "end value" : "1b0fsrnpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsrq0", "start inclusive" : true, "end value" : "1b0fsrq1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsrq4", "start inclusive" : true, "end value" : "1b0fsrq5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsrqh", "start inclusive" : true, "end value" : "1b0fsrqjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsrqn", "start inclusive" : true, "end value" : "1b0fsrqpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsrw0", "start inclusive" : true, "end value" : "1b0fsrw1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsrw4", "start inclusive" : true, "end value" : "1b0fsrw5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsrwh", "start inclusive" : true, "end value" : "1b0fsrwjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsrwn", "start inclusive" : true, "end value" : "1b0fsrwpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsry0", "start inclusive" : true, "end value" : "1b0fsry1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsry4", "start inclusive" : true, "end value" : "1b0fsry5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsryh", "start inclusive" : true, "end value" : "1b0fsryjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fsryn", "start inclusive" : true, "end value" : "1b0fsrypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2n0", "start inclusive" : true, "end value" : "1b0fu2n1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2n4", "start inclusive" : true, "end value" : "1b0fu2n5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2nh", "start inclusive" : true, "end value" : "1b0fu2njzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2nn", "start inclusive" : true, "end value" : "1b0fu2npzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2q0", "start inclusive" : true, "end value" : "1b0fu2q1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2q4", "start inclusive" : true, "end value" : "1b0fu2q5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2qh", "start inclusive" : true, "end value" : "1b0fu2qjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2qn", "start inclusive" : true, "end value" : "1b0fu2qpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2w0", "start inclusive" : true, "end value" : "1b0fu2w1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2w4", "start inclusive" : true, "end value" : "1b0fu2w5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2wh", "start inclusive" : true, "end value" : "1b0fu2wjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2wn", "start inclusive" : true, "end value" : "1b0fu2wpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2y0", "start inclusive" : true, "end value" : "1b0fu2y1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2y4", "start inclusive" : true, "end value" : "1b0fu2y5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2yh", "start inclusive" : true, "end value" : "1b0fu2yjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu2yn", "start inclusive" : true, "end value" : "1b0fu2ypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3n0", "start inclusive" : true, "end value" : "1b0fu3n1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3n4", "start inclusive" : true, "end value" : "1b0fu3n5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3nh", "start inclusive" : true, "end value" : "1b0fu3njzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3nn", "start inclusive" : true, "end value" : "1b0fu3npzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3q0", "start inclusive" : true, "end value" : "1b0fu3q1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3q4", "start inclusive" : true, "end value" : "1b0fu3q5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3qh", "start inclusive" : true, "end value" : "1b0fu3qjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3qn", "start inclusive" : true, "end value" : "1b0fu3qpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3w0", "start inclusive" : true, "end value" : "1b0fu3w1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3w4", "start inclusive" : true, "end value" : "1b0fu3w5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3wh", "start inclusive" : true, "end value" : "1b0fu3wjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3wn", "start inclusive" : true, "end value" : "1b0fu3wpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3y0", "start inclusive" : true, "end value" : "1b0fu3y1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3y4", "start inclusive" : true, "end value" : "1b0fu3y5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3yh", "start inclusive" : true, "end value" : "1b0fu3yjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu3yn", "start inclusive" : true, "end value" : "1b0fu3ypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6n0", "start inclusive" : true, "end value" : "1b0fu6n1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6n4", "start inclusive" : true, "end value" : "1b0fu6n5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6nh", "start inclusive" : true, "end value" : "1b0fu6njzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6nn", "start inclusive" : true, "end value" : "1b0fu6npzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6q0", "start inclusive" : true, "end value" : "1b0fu6q1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6q4", "start inclusive" : true, "end value" : "1b0fu6q5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6qh", "start inclusive" : true, "end value" : "1b0fu6qjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6qn", "start inclusive" : true, "end value" : "1b0fu6qpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6w0", "start inclusive" : true, "end value" : "1b0fu6w1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6w4", "start inclusive" : true, "end value" : "1b0fu6w5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6wh", "start inclusive" : true, "end value" : "1b0fu6wjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6wn", "start inclusive" : true, "end value" : "1b0fu6wpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6y0", "start inclusive" : true, "end value" : "1b0fu6y1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6y4", "start inclusive" : true, "end value" : "1b0fu6y5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6yh", "start inclusive" : true, "end value" : "1b0fu6yjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu6yn", "start inclusive" : true, "end value" : "1b0fu6ypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "1b0fu7n0", "start inclusive" : true, "end value" : "1b0fu7n0", "end inclusive" : true } }
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
        "value" : {"coordinates":[[-100,-89.6],[-100,-89.5]],"type":"LineString"}
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