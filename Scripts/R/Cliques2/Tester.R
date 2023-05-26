library(tidyverse)
library(sf)

distance <- function(x1, y1, x2, y2){
  x = x2 - x1
  y = y2 - y1
  x2 = x * x
  y2 = y * y
  return (sqrt(x2 + y2))
}
points = read_tsv("data/TT320_C500.tsv", col_names = c("x", "y", "cid", "oid", "tid")) |>
  filter(cid == 94) |>
  select(x, y, oid)

p1 = points
names(p1) = c("x1", "y1", "oid1")
p2 = points
names(p2) = c("x2", "y2", "oid2")
pairs0 = crossing(p1, p2) 
pairs1 = pairs0 |> filter(oid1 < oid2)
pairs2 = pairs1 |> mutate(dist = distance(x1,y1,x2,y2))
pairs3 = pairs2 |> filter(dist <= 5.001)

ggplot(points) + geom_point(aes(x=x, y=y)) +
  geom_point(data = pairs3, aes(x=x1, y=y1, colour = "blue")) +
  geom_point(data = pairs3, aes(x=x2, y=y2, colour = "red"))


