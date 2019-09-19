library(tidyverse)

data = read_tsv("/opt/GISData/points_sample.wkt", col_names = c("Index", "Coords")) %>%
  separate(Coords, into = c("x", "y"), sep = ",") %>%
  mutate(x = as.numeric(str_trim(x)), y = as.numeric(str_trim(y)))

p = ggplot(data, aes(x=x, y=y, group=1)) + geom_point()
plot(p)

