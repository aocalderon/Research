library(tidyverse)
library(latex2exp)

TINSTANCE = 321
fields <- c("ts","start","host", "tag", "z", "appId","partitions","dataset","epsilon","mu","delta","method","stage","time")  

data0 <- enframe(read_lines( "psi_runner_datasets1.txt" ), value = "line") |>
  separate(col = line, into = fields, sep = "\\|") |>
  mutate(epsilon = as.numeric(epsilon), time = as.numeric(time)) |>
  separate(col = dataset, into = c("tinstance", "cellId"), sep = "_") |>
  select(tinstance, cellId, method, epsilon, time) |>
  separate(col = tinstance, into = c(NA, "tinstance"), sep = "T") |>
  separate(col = cellId, into = c(NA, "cellId"), sep = "cell") |>
  mutate(tinstance = as.numeric(tinstance), cellId = as.numeric(cellId)) |>
  filter(tinstance == TINSTANCE) |>
  select(cellId, method, epsilon, time) |>
  group_by(cellId, method, epsilon) |> 
  summarise(time = mean(time)) 

cells_prime <- read_tsv("psi_runner_datasets_cells.tsv", col_names = c("wkt", "cid", "area", "n1", "d1", "n2", "d2")) 
if(TINSTANCE == 320){
  cells <- cells_prime |> mutate(n = n1, d = d1) |> select(cid, area, n, d)
} else {
  cells <- cells_prime |> mutate(n = n2, d = d2) |> select(cid, area, n, d)
}

data <- data0 |> 
  inner_join(cells, by = join_by(cellId == cid)) |>
  select(cellId, method, n, d, epsilon, time) 

p = ggplot(data, aes(x = d, y = time, shape = method, color = method)) +
  geom_point() +
  labs(x=TeX("Density (points / $m^2$)"), y="Time(s)") +
  facet_wrap(~epsilon) +
  theme_bw() 
plot(p)  

W = 6
H = 4
ggsave(paste0("psi_runner_", TINSTANCE, ".pdf"), width = W, height = H)
