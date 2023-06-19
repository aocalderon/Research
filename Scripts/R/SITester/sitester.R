library(tidyverse)

data = enframe(read_lines("~/Research/Scripts/Scala/SITester/sitester6.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = c("ts","start","tag","operation","metric","type","time"), sep = "\\|") |>
  select(operation, metric, type, time) |>
  mutate(time = as.numeric(time), metric = as.numeric(metric),
         type = as.factor(type)) |>
  group_by(operation, metric, type) |> summarise(time = mean(time))

data1 = data |> filter(operation == "Insertion") |> filter(metric != 5000)
p = ggplot(data1, aes(x = metric, y = time, group = type, color = type, shape = type)) +
  geom_line(aes(linetype = type)) + geom_point(size = 2.5) +
  scale_color_discrete("Implementation") +
  scale_shape_manual("Implementation", values = 1:nlevels(data1$type)) +
  guides(linetype = "none") +
  labs(x="Insertion(# of points)", y="Time(s)") +
  theme_bw()
plot(p)  

W = 6
H = 4
ggsave(paste0("insertion.pdf"), width = W, height = H)

data1 = data |> filter(operation == "Query") |> filter(metric != 10) |> filter(type != "HPRtree ")
p = ggplot(data1, aes(x = metric, y = time, group = type, color = type, shape = type)) + 
  geom_line(aes(linetype = type)) + geom_point(size = 2.5) +
  scale_color_discrete("Implementation") +
  scale_shape_manual("Implementation", values = 1:nlevels(data1$type)) +
  guides(linetype = "none") +
  labs(x="Insertion(# of points)", y="Time(s)") +
  theme_bw()
plot(p) 
ggsave(paste0("queryPairs.pdf"), width = W, height = H)
