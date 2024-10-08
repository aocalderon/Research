report_Official <- structure(list(Competency.Official.Rating = structure(c(1L, 2L, 
                                                                           3L, 1L, 2L, 3L, 1L, 2L, 3L, 1L, 2L, 3L, 1L, 2L, 3L, 1L, 2L, 3L
), .Label = c("demonstrates the value", "development area", "role model"
), class = "factor"), Competency.Name = structure(c(1L, 1L, 1L, 
                                                    2L, 2L, 2L, 3L, 3L, 3L, 4L, 4L, 4L, 5L, 5L, 5L, 6L, 6L, 6L), .Label = c("Co-creating the future", 
                                                                                                                            "Feedback", "Impact", "One company", "One voice", "Simplification"
                                                    ), class = "factor"), Freq = c(305L, 114L, 70L, 352L, 72L, 80L, 
                                                                                   333L, 88L, 80L, 293L, 38L, 167L, 313L, 20L, 171L, 358L, 59L, 
                                                                                   85L)), class = "data.frame", row.names = c(NA, -18L))

ggplot(report_Official, aes(x = Competency.Name, y = Freq, 
                            fill = factor(Competency.Official.Rating, levels = c("role model", "demonstrates the value", "development area")),
                            label = Freq)) +
  geom_bar(stat = "identity") +  # position = fill will give the %; stack will give #of people
  geom_text(size = 5, position = position_stack(vjust = 0.5))   + 
  #   facet_wrap(  ~ Subject.Direction) +
  labs(x = "Competence Name", 
       y = "Number of Employees") + # or Percentage of Employees [%] or Number of Employees
  theme_bw()  +
  ggtitle("Behavior Official Rating") +
  theme(panel.grid.major.x = element_blank(),
        panel.grid.major.y = element_line(colour = "grey50"),
        plot.title = element_text(size = rel(1.5),
                                  face = "bold", vjust = 1.5),
        axis.title = element_text(face = "bold"),
        legend.key.size = unit(0.5, "cm"),
        #legend.box.background = element_rect(color="red", size=2),
        #legend.box.margin = margin(116, 6, 6, 6),
        legend.key = element_rect(colour = "gray"),
        axis.title.y = element_text(vjust= 1.8),
        axis.title.x = element_text(vjust= -0.5)) +
  # scale_fill_manual(name="Manager Rating", values = wes_palette("Zissou1", n = 5)) +
  scale_fill_brewer(palette = "PuBu")+ #Oranges for Values
  guides(fill=guide_legend(title="Competency Official Rating")) +
  theme(panel.grid.major = element_blank(), text = element_text(size=15), panel.grid.minor = element_blank(),
        panel.background = element_blank(), axis.line = element_line(colour = "black")) +
  theme(axis.text.x = element_text(face = "bold",
                                   size = 15, angle = 45, hjust = 1),
        axis.text.y = element_text(face = "bold",
                                   size = 15, angle = 90))

rm <- structure(list(Competency.Self.Rating = structure(c(3L, 3L, 3L, 
                                                          3L, 3L, 3L), .Label = c("demonstrates the value", "development area", 
                                                                                  "role model"), class = "factor"), Competency.Name = structure(1:6, .Label = c("Co-creating the future", 
                                                                                                                                                                "Feedback", "Impact", "One company", "One voice", "Simplification"
                                                                                  ), class = "factor"), Freq = c(75L, 94L, 113L, 180L, 189L, 116L
                                                                                  )), row.names = c(3L, 6L, 9L, 12L, 15L, 18L), class = "data.frame")

dv <- structure(list(Competency.Self.Rating = structure(c(1L, 1L, 1L, 
                                                          1L, 1L, 1L), .Label = c("demonstrates the value", "development area", 
                                                                                  "role model"), class = "factor"), Competency.Name = structure(1:6, .Label = c("Co-creating the future", 
                                                                                                                                                                "Feedback", "Impact", "One company", "One voice", "Simplification"
                                                                                  ), class = "factor"), Freq = c(309L, 337L, 334L, 286L, 294L, 
                                                                                                                 338L)), row.names = c(1L, 4L, 7L, 10L, 13L, 16L), class = "data.frame")

da <- structure(list(Competency.Self.Rating = structure(c(2L, 2L, 2L, 
                                                          2L, 2L, 2L), .Label = c("demonstrates the value", "development area", 
                                                                                  "role model"), class = "factor"), Competency.Name = structure(1:6, .Label = c("Co-creating the future", 
                                                                                                                                                                "Feedback", "Impact", "One company", "One voice", "Simplification"
                                                                                  ), class = "factor"), Freq = c(105L, 73L, 54L, 32L, 21L, 48L)), row.names = c(2L, 
                                                                                                                                                                5L, 8L, 11L, 14L, 17L), class = "data.frame")


p = ggplot(report_Official, aes(x = Competency.Name, 
                            y = Freq, 
                            label = Freq)) +
  geom_bar(data = report_Official,
           aes(fill = factor(Competency.Official.Rating,
                             
                             levels = c("role model", "demonstrates the value", "development area"))),
           stat = "identity") +  # position = fill will give the %; stack will give #of people

  geom_point(data = rm,
             color = "black") +
  geom_line(data = rm, 
            aes(group = 1)) +
  geom_point(data = dv,
             color = "red") +
  geom_line(data = dv,
            aes(group = 1),
            color = "red") +
  geom_point(data = da,
             color = "blue") +
  geom_line(data = da,
            aes(group = 1),
            color = "blue") +
  labs(x = "Competence Name", 
       y = "Number of Employees") + 
  theme_bw()  +
  ggtitle("Behavior Official Rating") +
  theme(panel.grid.major.x = element_blank(),
        panel.grid.major.y = element_line(colour = "grey50"),
        plot.title = element_text(size = rel(1.5),
                                  face = "bold", vjust = 1.5),
        axis.title = element_text(face = "bold"),
        legend.key.size = unit(0.5, "cm"),
        
        legend.key = element_rect(colour = "gray"),
        axis.title.y = element_text(vjust= 1.8),
        axis.title.x = element_text(vjust= -0.5)) +
  scale_fill_brewer(palette = "PuBu")+ #Oranges for Values
  guides(fill=guide_legend(title="Competency Official Rating")) 
plot(p)