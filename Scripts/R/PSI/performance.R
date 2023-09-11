library(tidyverse)
library(latex2exp)
library(xtable)

fields = c("ts", "start", "host", "tag", "n", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "stage", "time")
data <- enframe(read_lines( "~/Research/Scripts/Scala/PFlock/nohup.out" ), value = "line") |>
  filter(str_detect(line, 'TIME')) |>  
  separate(col = line, into = fields, sep = "\\|") |>
  select(method, stage, epsilon, time) |>
  mutate(epsilon = as.numeric(epsilon), time = as.numeric(time))
  
methods = data |> group_by(method, epsilon) |> summarise(time = mean(time))

p = ggplot(methods, aes(x = as.factor(epsilon), y = time, fill = method)) +
  geom_bar(stat = "identity", position = 'dodge') +
  labs(x="Epsilon(m)", y="Time(s)") +
  theme_bw()
plot(p)  

W = 6
H = 4
ggsave(paste0("performance.pdf"), width = W, height = H)

stages = data |> filter(method == "PSI") |> 
  group_by(stage, epsilon) |> 
  summarise(time = mean(time)) |>
  filter(stage != "Total     ")

stages$stage <- recode_factor(stages$stage, FC="Filter candidates", PS="Plane sweeping")

g = ggplot(stages, aes(x = as.factor(epsilon), y = time, fill = stage)) +
  geom_bar(stat = "identity", position = 'dodge') +
  labs(x="Epsilon(m)", y="Time(s)") +
  theme_bw()
plot(g)  

W = 6
H = 4
ggsave(paste0("performancePSI.pdf"), width = W, height = H)
