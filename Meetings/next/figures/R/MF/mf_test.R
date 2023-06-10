library(tidyverse)

fields = c("ts","start","tag","appId","epsilon","mu","delta","method", "capacity","stage", "time")
test = enframe(read_lines("mf_test.txt"), value="line")

data1 = test |>  filter(str_detect(line, 'TIME')) |>
  separate(line, into = fields, sep = "\\|") |>
  select(epsilon, capacity, method, stage, time) |>
  mutate(epsilon = as.numeric(epsilon), capacity = as.numeric(capacity), 
         time = as.numeric(time), stage = str_trim(stage)) 

bfe = data1 |> filter(method == "BFE" & stage == "Total") |>
  select(epsilon, time) |>
  group_by(epsilon) |> summarise(time = mean(time)) 

p = ggplot(bfe, aes(x = as.factor(epsilon), y = time)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Epsilon(m)", y="Time(s)") 
plot(p)

W = 6
H = 4
ggsave(paste0("bfe.pdf"), width = W, height = H)

mf = data1 |> filter(method == "PBFE" & stage == "Run") |>
  select(epsilon, capacity, time) |>
  group_by(epsilon, capacity) |> summarise(time = mean(time)) 

p = ggplot(mf, aes(x = as.factor(capacity), y = time, fill = as.factor(epsilon))) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Epsilon(m)", y="Time(s)") 
plot(p)

W = 6
H = 4
#ggsave(paste0("pflock.pdf"), width = W, height = H)

data = bfe |> add_column(method = "BFE") |> 
  bind_rows(mf |> filter(capacity == 100) |> select(epsilon, time) |> 
              add_column(method = "PFlocks"))
p = ggplot(data, aes(x = as.factor(epsilon), y = time, fill = method)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Epsilon(m)", y="Time(s)") 
plot(p)

W = 6
H = 4
ggsave(paste0("PFlocksVsBFE.pdf"), width = W, height = H)

