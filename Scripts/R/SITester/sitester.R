library(tidyverse)

data = enframe(read_lines("~/Research/Scripts/Scala/SITester/sitester2.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = c("ts","start","tag","operation","metric","type","time"), sep = "\\|") |>
  select(operation, metric, type, time) |>
  mutate(time = as.numeric(time), metric = as.numeric(metric)) |>
  group_by(operation, metric, type) |> summarise(time = mean(time))

p = ggplot(data |> filter(operation == "Insertion" && metric != 5000), aes(x = metric, y = time, fill = type)) +
  geom_line(aes(linetype = type, color = type)) + 
  geom_point(aes(shape = type, size = 5, color = type), size = 2.5) +
  labs(x="Insertion(# of points)", y="Time(s)") +
  theme_bw()
plot(p)  
W = 6
H = 4
ggsave(paste0("insertion.pdf"), width = W, height = H)

p = ggplot(data |> filter(operation == "Query" && metric != 1000), aes(x = as.factor(metric), y = time, group = type)) +
  geom_line(aes(linetype = type, color = type)) + 
  geom_point(aes(shape = type, color = type), size = 2.5) +
  labs(x="Range query(m)", y="Time(s)") +
  theme_bw()
plot(p)  
ggsave(paste0("query.pdf"), width = W, height = H)
