library(tidyverse)
library(xtable)

data = read_tsv("hoods518.tsv", col_names = c("key", "coords", "n", "time"))

p = ggplot(data, aes(x = as.factor(key), y = time)) + geom_col() + 
  labs(x="Grid Cell Id", y="Time(s)") + theme_bw()
plot(p)

W = 6
H = 4
ggsave(paste0("hoods518_time.pdf"), width = W, height = H)

p = ggplot(data, aes(x = as.factor(key), y = n)) + geom_col() + 
  labs(x="Grid Cell Id", y="Number of points") + theme_bw()
plot(p)
ggsave(paste0("hoods518_n.pdf"), width = W, height = H)

stats = enframe(read_lines("stats518.txt"), value = "line") |>
  separate(col = line, into = c(NA, "info"), sep = "BFE") |>
  separate(col = info, into = c(NA, "key", "value"), sep = "\\|")


print(xtable(stats |> select(key, value)), include.rownames=FALSE)

print(xtable(data |> arrange(desc(time)) |> head(10)))
