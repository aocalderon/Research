# title = "Execution time by delta value."
# g = ggplot(data=dataT, aes(x=factor(Epsilon), y=Time, fill=Method)) +
#   geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
#   labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) +
#   facet_wrap(~Delta) +
#   ylim(0,400)
# svg("/home/and/Documents/PhD/Research/Experiments/FlockFinder_v1.0/SJvsMLEpsilon.svg", width=W, height=H)
# plot(g)
# dev.off()
# 
# title = "Execution time by epsilon value."
# g = ggplot(data=dataT, aes(x=factor(Delta), y=Time, fill=Method)) +
#   geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
#   labs(title=title, y="Time(s)", x=expression(paste(delta,"(timestamps)"))) +
#   facet_wrap(~Epsilon) +
#   ylim(0,400)
# svg("/home/and/Documents/PhD/Research/Experiments/FlockFinder_v1.0/SJvsMLDelta.svg", width=W, height=H)
# plot(g)
# dev.off()
