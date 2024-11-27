library(tidyverse)
library(latex2exp)
library(ggpubr)

#prefix = "dense_"
#suffix = "_crowded_prime"
prefix = "cell187_"
suffix = ""
fields <- c("ts","t","host","tag","n","AppId","x","dataset","epsilon","mu","delta","method","total","time")
data <- enframe(read_lines("cmbc_cell187.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Total')) |>
  separate(col = line, into = fields, sep = "\\|") |>
  select(dataset, epsilon, method, time) |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon)) |>
  group_by(dataset, method, epsilon) |> summarise(time = mean(time))

# data |> write_tsv("cell187.tsv", col_names = T)
data <- read_tsv("cell187.tsv", col_names = T)

data_prime <- data |> filter(dataset == paste0(prefix, "500", suffix))
p500 = ggplot(data_prime, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(title="(a) 500", x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()

data_prime <- data |> filter(dataset == paste0(prefix, "2K", suffix))
p1K = ggplot(data_prime, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(title="(b) 1K", x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()

data_prime <- data |> filter(dataset == paste0(prefix, "4K", suffix))
p2K = ggplot(data_prime, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(title="(c) 2K", x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()

data_prime <- data |> filter(dataset == paste0(prefix, "6K", suffix))
p4K = ggplot(data_prime, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(title="(d) 4K", x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()

data_prime <- data |> filter(dataset == paste0(prefix, "8K", suffix))
p6K = ggplot(data_prime, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(title="(e) 6K", x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()

data_prime <- data |> filter(dataset == paste0(prefix, "10K", suffix))
p8K = ggplot(data_prime, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(title="(f) 8K", x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()

ggarrange(p500, p1K, p2K, p4K, p6K, p8K, ncol=3, nrow=2, common.legend = TRUE, legend="bottom")
ggsave(paste0("cmbc.pdf"), width = 10, height = 6)