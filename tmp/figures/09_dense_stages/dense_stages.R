library(tidyverse)
library(latex2exp)

# fields <- c("ts","start","host", "tag", "z", "appId","partitions","dataset","epsilon","mu","delta","method","stage","time")  
# bfe <- enframe(read_lines( "psi_benchmark2.txt" ), value = "line") |>
#   filter(str_detect(line, 'TIME')) |>
#   separate(col = line, into = fields, sep = "\\|") |>
#   mutate(Stage = as.factor(str_trim(stage)), epsilon = as.numeric(epsilon), time = as.numeric(time)) |>
#   filter(dataset == "cell187" & method == "BFE") |>
#   filter(epsilon <= 20) |>
#   select(epsilon, Stage, time) |> 
#   group_by(epsilon, Stage) |> summarise(time = mean(time)) 
bfe <- read_tsv("bfe.tsv")

p = ggplot(bfe, aes(x = as.factor(epsilon), y = time, group = Stage, color = Stage, shape = Stage, linetype = Stage)) +
  labs(x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  geom_line() + 
  geom_point(size = 2.5) +
  theme_bw() 
plot(p)  

W = 8
H = 6
ggsave(paste0("dense_stages_bfe.pdf"), width = W, height = H)

# psi <- enframe(read_lines( "psi_benchmark2.txt" ), value = "line") |>
#   filter(str_detect(line, 'TIME')) |>
#   separate(col = line, into = fields, sep = "\\|") |>
#   mutate(Stage = as.factor(str_trim(stage)), epsilon = as.numeric(epsilon), time = as.numeric(time)) |>
#   filter(dataset == "cell187" & method == "PSI") |>
#   filter(epsilon <= 20) |>
#   select(epsilon, Stage, time) |> 
#   group_by(epsilon, Stage) |> summarise(time = mean(time)) 
psi <- read_tsv("psi.tsv")

p = ggplot(psi, aes(x = as.factor(epsilon), y = time, group = Stage, color = Stage, shape = Stage, linetype = Stage)) +
  labs(x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  geom_line() + 
  geom_point(size = 2.5) +
  theme_bw() 
plot(p)

ggsave(paste0("dense_stages_psi.pdf"), width = W, height = H)

data <- bfe |> mutate(method = "BFE") |> bind_rows( psi |> mutate(method = "PSI") )
p = ggplot(data, aes(x = as.factor(epsilon), y = time, group = Stage, color = Stage, shape = Stage, linetype = Stage)) +
  labs(x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  geom_line() + 
  geom_point(size = 2.5) +
  facet_wrap(~method) +
  theme_bw() 
plot(p)
