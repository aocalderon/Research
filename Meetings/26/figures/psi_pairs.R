library(tidyverse)
library(latex2exp)

fields <- c("ts","start","host", "tag", "z", "appId","partitions","dataset","epsilon","mu","delta","method","stage","time")  
times <- enframe(read_lines( "psi_benchmark3_times.txt" ), value = "line") |>
  separate(col = line, into = fields, sep = "\\|") |>
  separate(col = dataset, into = c("tinstance", "cellId"), sep = "_") |>
  select(tinstance, cellId, method, epsilon, time) |>
  separate(col = tinstance, into = c(NA, "tinstance"), sep = "T") |>
  separate(col = cellId, into = c(NA, "cellId"), sep = "cell") |>
  mutate(tinstance = as.numeric(tinstance), 
         cellId    = as.numeric(cellId), 
         epsilon   = as.numeric(epsilon), 
         time      = as.numeric(time)) |>
  select(tinstance, cellId,  epsilon, method, time) |>
  group_by(tinstance, cellId, method, epsilon) |> 
  summarise(time = mean(time)) 

fields <- c("ts","start","host", "tag", "z", "appId","partitions","dataset","epsilon","mu","delta","method","stage","n")  
pairs <- enframe(read_lines( "psi_benchmark3_pairs.txt" ), value = "line") |>
  separate(col = line, into = fields, sep = "\\|") |>
  separate(col = dataset, into = c("tinstance", "cellId"), sep = "_") |>
  select(tinstance, cellId, epsilon, n) |>
  separate(col = tinstance, into = c(NA, "tinstance"), sep = "T") |>
  separate(col = cellId, into = c(NA, "cellId"), sep = "cell") |>
  mutate(tinstance = as.numeric(tinstance), 
         cellId    = as.numeric(cellId), 
         epsilon   = as.numeric(epsilon), 
         n         = as.numeric(n)) |>
  select(tinstance, cellId, epsilon, n) |>
  group_by(tinstance, cellId, epsilon) |> 
  summarise(n = max(n)) 

data <- times |> inner_join(pairs, by = c("tinstance", "cellId", "epsilon"))

p = ggplot(data, aes(x = n, y = time, shape = method, color = method)) +
  geom_point() +
  ggtitle(paste("Number of pairs performance by time instance and epsilon(m)")) + 
  ylim(0, 0.65) +
  labs(x=TeX("Pairs per cell"), y="Time(s)") +
  facet_wrap(~ tinstance + epsilon) +
  theme_bw() 
plot(p)  

W = 18
H = 12
ggsave(paste0("psi_pairs.pdf"), width = W, height = H)

pairs |> 
  filter(epsilon == 20) |> 
  select(tinstance, cellId, n) |>
  write_tsv("npairs.tsv", col_names = F)
