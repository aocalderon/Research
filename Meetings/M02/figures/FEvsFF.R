require(tidyverse)

data = read_csv("LA25K.csv", col_names = F) %>% rename(Method = X1, Epsilon = X2, Time = X3)

head(data)

p = ggplot(data = data, aes(x = factor(Epsilon), y = Time, group = Method)) +
  geom_line(aes(linetype = Method, color = Method)) +
  geom_point(aes(shape = Method, color = Method)) + 
  labs(title="LA 25K Dataset", x="Epsilon [m]", y="Time [s]") +
  theme(legend.position="top")
plot(p)

ggsave("LA25K.pdf", width = 7, height = 5, device = "pdf")