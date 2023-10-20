library(tidyverse)
library(latex2exp)

fields <- c("ts","start","host", "tag", "z", "appId","partitions","dataset","epsilon","mu","delta","method","stage","time")  
data0 <- enframe(read_lines( "psi_test_v2.txt" ), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields, sep = "\\|") |>
  select(appId, epsilon, mu, method, stage, time) |>
  mutate(stage = str_trim(stage), epsilon = as.numeric(epsilon), 
         mu = as.numeric(mu), time = as.numeric(time))

data <- data0 |> filter(stage != 'Total' & stage != 'PS' & stage != 'FC') |>
  group_by(appId, epsilon, mu, method, stage) |> summarise(time = mean(time)) |>
  group_by(appId, epsilon, mu, method) |> summarise(time = sum(time))

dataM3 <- data |> filter(mu == 3) |> select(epsilon, method, time) |> 
  group_by(epsilon, method) |> summarise(time = mean(time))
dataE10 <- data |> filter(epsilon == 10) |> select(mu, method, time) |> 
  group_by(mu, method) |> summarise(time = mean(time))


p = ggplot(dataE10, aes(x = as.factor(mu), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(x=TeX("$\\mu(n)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()
plot(p)  

p = ggplot(dataM3, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()
plot(p)  

fields <- c("tag", "epsilon", "mu", "delta", "time", "n")
datap <- enframe(read_lines( "psi_test_v3.txt" ), value = "line") |>
  filter(str_detect(line, 'PSICPP')) |>
  separate(col = line, into = fields, sep = "\\|") |>
  select(epsilon, mu, time) |>
  mutate(epsilon = as.numeric(epsilon), mu = as.numeric(mu), time = as.numeric(time)) |>
  filter(mu == 3) |>
  mutate(method = "PSI_CPP") |>
  group_by(epsilon, method) |> summarise(time = mean(time))

dataPP <- dataM3 |> bind_rows(datap)

p = ggplot(dataPP, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()
plot(p)  

W = 6
H = 4
ggsave(paste0("psi_bfe_psicpp.pdf"), width = W, height = H)

p = ggplot(dataPP |> filter(method != "PSI"), aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()
plot(p)  

W = 6
H = 4
ggsave(paste0("bfe_psicpp.pdf"), width = W, height = H)


fields <- c("ts","start","host", "tag", "z", "appId","partitions","dataset","epsilon","mu","delta","method","metric","pairs")  
pairs <- enframe(read_lines( "pairs.txt" ), value = "line") |>
  filter(str_detect(line, 'Pairs')) |>
  separate(col = line, into = fields, sep = "\\|") |>
  select(method, epsilon, pairs) |>
  mutate( epsilon = as.numeric(epsilon), pairs = as.numeric(pairs))
  

p = ggplot(pairs, aes(x = factor(epsilon), y = pairs, fill = method)) +
  geom_col(position = "dodge") +
  labs(x=TeX("$\\epsilon(m)$"), y="number of pairs") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  theme_bw()
plot(p)
