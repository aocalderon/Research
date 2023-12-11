library(tidyverse)
library(latex2exp)

TINSTANCE = 321
fields <- c("ts","start","host", "tag", "z", "appId","partitions","dataset","epsilon","mu","delta","method","stage","time")  

data0 <- enframe(read_lines( "psi_benchmark3_times.txt" ), value = "line") |>
  separate(col = line, into = fields, sep = "\\|") |>
  mutate(epsilon = as.numeric(epsilon), time = as.numeric(time)) |>
  separate(col = dataset, into = c("tinstance", "cid"), sep = "_") |>
  select(tinstance, cid, method, epsilon, time) |>
  separate(col = tinstance, into = c(NA, "tinstance"), sep = "T") |>
  separate(col = cid, into = c(NA, "cid"), sep = "cell") |>
  mutate(tinstance = as.numeric(tinstance), cid = as.numeric(cid)) |>
  filter(tinstance == TINSTANCE) |>
  select(cid, method, epsilon, time) |>
  group_by(cid, method, epsilon) |> 
  summarise(time = mean(time)) 

cells_prime <- read_tsv("psi_runner_datasets_cells.tsv", col_names = c("wkt", "cid", "area", "n1", "d1", "n2", "d2")) 
if(TINSTANCE == 320){
  cells <- cells_prime |> mutate(n = n1, d = d1) |> select(cid, area, n, d)
} else {
  cells <- cells_prime |> mutate(n = n2, d = d2) |> select(cid, area, n, d)
}

data <- data0 |> 
  inner_join(cells, by = c("cid")) |>
  select(cid, method, n, d, epsilon, time) 

p = ggplot(data, aes(x = n, y = time, shape = method, color = method)) +
  geom_point() +
  ggtitle(paste("Number of points at different values of epsilon(m)\nTime instance: ", TINSTANCE)) + 
  ylim(0, 0.65) +
  labs(x=TeX("Points per cell"), y="Time(s)") +
  facet_wrap(~epsilon) +
  theme_bw() 
plot(p)  

W = 18
H = 12
ggsave(paste0("psi_count_", TINSTANCE, ".pdf"), width = W, height = H)
