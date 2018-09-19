require(ggplot2)

data = read.table('/home/and/Documents/PhD/Research/Validation/LCM_max/times003.txt')
n = nrow(data)
method = rep(c('FPMax', 'LCM'), n/2)
dataset = rep(seq(1,n/2), each=2)
counts = data.frame(Dataset = dataset, Method = method, Time = data$V4 / 1000)
#counts = counts[counts$Time > 2, ]

g = ggplot(data=counts, aes(x=Dataset, y=Time, color=Method)) +
  geom_line(alpha=0.8) +
  geom_hline(yintercept = mean(counts[counts$Method=='FPMax', "Time"]), linetype="dashed", color='red') +
  geom_hline(yintercept = mean(counts[counts$Method=='LCM', "Time"]), linetype="dashed", color='blue')

plot(g)