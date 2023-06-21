library(tidyverse)

data = enframe(read_lines( pipe( 'ssh acald013@hn "cat /home/acald013/Research/Scripts/Scala/Cliques/output.log"' )), value = "line") |>
  filter(str_detect(line, 'MAXIMALS')) |>
  separate(col = line, into = c("ts","start","tag", "cellId", "appId","partitions","dataset","epsilon","mu","delta","project","maximals","stage","time"), sep = "\\|") |>
  select(cellId, stage, time) |>
  mutate(time = as.numeric(time), cellId = as.numeric(cellId)) |>
  group_by(cellId, stage) |> summarise(time = sum(time))

p = ggplot(data, aes(x = as.factor(cellId), y = time, fill = stage)) +
  geom_col() +
  scale_fill_discrete("Stage") +
  labs(x="Cell ID", y="Time(s)") +
  theme_bw()
plot(p)  

W = 6
H = 4
ggsave(paste0("performance.pdf"), width = W, height = H)
