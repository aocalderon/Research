source("~/Documents/PhD/Research/Validation/FFvsFE/FF.R")
source("~/Documents/PhD/Research/Validation/FFvsFE/FE.R")

data = rbind(FE, FF)

p = ggplot(data = data, aes(x = factor(Epsilon), y = Time, group = Method)) +
  geom_line(aes(linetype = Method, color = Method)) +
  geom_point(aes(shape = Method, color = Method)) + 
  labs(title="LA 25K Dataset", x="Epsilon [m]", y="Time [s]") +
  theme(legend.position="top")
plot(p)

ggsave("~/Documents/PhD/Research/Validation/FFvsFE/LA25K.pdf", width = 10, height = 6, device = "pdf")