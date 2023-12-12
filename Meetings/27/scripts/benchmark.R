library(tidyverse)
library(latex2exp)

fields <- c("ts","start","host", "tag", "z", "appId","partitions","dataset","epsilon","mu","delta","method","stage","time")  

data <- enframe(read_lines("nohup.out"), value = "line") |>
  filter(str_detect(line, '\\|Total')) |>
  separate(col = line, into = fields, sep = "\\|") |>
  mutate(epsilon = as.numeric(epsilon), mu = as.numeric(mu), delta = as.numeric(delta), time = as.numeric(time)) |>
  select(epsilon, mu, delta, time) |>
  group_by(epsilon, mu, delta) |>
  summarise(time = mean(time))

data

epsilon_default <- 10
mu_default <- 3
delta_default <- 5
W <- 12
H <- 8

data_epsilon <- data |> 
  filter(mu == mu_default & delta == delta_default) |>
  select(epsilon, time)

p_epsilon <- ggplot(data_epsilon, aes(x = as.factor(epsilon), y = time, group = 1)) +
  geom_line(linetype = 2) + 
  geom_point(size = 2) +
  labs(x=TeX("$\\epsilon$"), y="Time(s)") +
  guides(linetype = "none") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))
plot(p_epsilon)  
ggsave(paste0("epsilon_benchmark.pdf"), width = W, height = H)

data_mu <- data |> 
  filter(epsilon == epsilon_default & delta == delta_default) |>
  select(mu, time)

p_mu <- ggplot(data_mu, aes(x = as.factor(mu), y = time, group = 1)) +
  geom_line(linetype = 2) + 
  geom_point(size = 2) +
  labs(x=TeX("$\\mu$"), y="Time(s)") +
  guides(linetype = "none") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))
plot(p_mu)  
ggsave(paste0("mu_benchmark.pdf"), width = W, height = H)

data_delta <- data |> 
  filter(epsilon == epsilon_default & mu == mu_default) |>
  select(delta, time)

p_delta <- ggplot(data_delta, aes(x = as.factor(delta), y = time, group = 1)) +
  geom_line(linetype = 2) + 
  geom_point(size = 2) +
  labs(x=TeX("$\\delta$"), y="Time(s)") +
  guides(linetype = "none") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))
plot(p_delta) 
ggsave(paste0("delta_benchmark.pdf"), width = W, height = H)
