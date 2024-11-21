library(tidyverse)
library(latex2exp)
library(ggpubr)

fields <- c("ts","t","host","tag","n","AppId","x","dataset","epsilon","mu","delta","method","total","time")
data <- enframe(read_lines("cmbc.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Total')) |>
  separate(col = line, into = fields, sep = "\\|") |>
  select(dataset, epsilon, method, time) |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon)) |>
  group_by(dataset, method, epsilon) |> summarise(time = mean(time))

data_prime <- data |> filter(dataset == "dense_2K_crowded_prime")
p2K = ggplot(data_prime, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(title="(a) 2K", x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()

data_prime <- data |> filter(dataset == "dense_4K_crowded_prime")
p4K = ggplot(data_prime, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(title="(b) 4K", x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()

data_prime <- data |> filter(dataset == "dense_6K_crowded_prime")
p6K = ggplot(data_prime, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(title="(c) 6K", x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()

data_prime <- data |> filter(dataset == "dense_8K_crowded_prime")
p8K = ggplot(data_prime, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(title="(d) 8K", x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()

data_prime <- data |> filter(dataset == "dense_10K_crowded_prime")
p10K = ggplot(data_prime, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(title="(e) 10K", x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()

ggarrange(p2K, p4K, p6K, p8K, p10K, ncol=3, nrow=2, common.legend = TRUE, legend="bottom")
ggsave(paste0("cmbc.pdf"), width = 8, height = 6)