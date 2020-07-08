library(tidyverse)
library(lubridate)
library(scales)
library(Cairo)

# Alternatively you can put all this in a CSV file with the same columns and
# then load it with read_csv()
# tasks <- read_csv("path/to/the/file")

tasks <- tribble(
  ~Start,       ~End,         ~Project,          ~Task,
  "2015-11-15", "2015-11-20", "Data collection", "Use IssueCrawler to expand lists",
  "2015-11-21", "2015-11-25", "Data collection", "Complete INGO databases",
  "2015-11-16", "2015-12-15", "Data collection", "Find all INGO legislation",
  "2015-12-15", "2015-12-25", "Data collection", "Code INGO restrictions",
  "2015-11-15", "2015-11-25", "Data collection", "Develop general INGO survey",
  "2015-11-25", "2015-12-31", "Data collection", "Administer survey",
  "2015-12-25", "2015-12-31", "Data analysis",   "Model stability and restrictions",
  "2016-01-01", "2016-02-02", "Writing",         "Chapter on formal restrictions (H1)",
  "2016-01-15", "2016-02-15", "Writing",         "Paper or chapter on survey results (H3 and H4)",
  "2016-03-16", "2016-03-19", "Writing",         "ISA conference in Atlanta",
  "2016-02-01", "2016-03-01", "Data collection", "Historical INGO restrictions in China",
  "2016-02-01", "2016-03-01", "Data collection", "Historical INGO restrictions in Egypt",
  "2016-05-01", "2016-06-01", "Data collection", "Fieldwork in London and Beijing",
  "2016-03-01", "2016-04-01", "Data analysis",   "Analyze restrictions in China and Egypt",
  "2016-04-01", "2016-06-01", "Data analysis",   "Analyze INGO activities",
  "2016-04-01", "2016-06-15", "Writing",         "Chapter on application of restrictions (H2)",
  "2016-05-01", "2016-07-01", "Writing",         "Chapter on INGO ideal points (H3)",
  "2016-05-01", "2016-07-01", "Writing",         "Chapter on INGO flexibility (H4)",
  "2016-07-01", "2016-08-01", "Writing",         "Theory",
  "2016-08-01", "2016-09-15", "Writing",         "Conclusion",
  "2016-08-01", "2016-09-15", "Writing",         "Introduction"
)

# Convert data to long for ggplot
tasks.long <- tasks %>%
  mutate(Start = ymd(Start),
         End = ymd(End)) %>%
  gather(date.type, task.date, -c(Project, Task)) %>%
  arrange(date.type, task.date) %>%
  mutate(Task = factor(Task, levels=rev(unique(Task)), ordered=TRUE))

# Custom theme for making a clean Gantt chart
theme_gantt <- function(base_size=11, base_family="Source Sans Pro Light") {
  ret <- theme_bw(base_size, base_family) %+replace%
    theme(panel.background = element_rect(fill="#ffffff", colour=NA),
          axis.title.x=element_text(vjust=-0.2), axis.title.y=element_text(vjust=1.5),
          title=element_text(vjust=1.2, family="Source Sans Pro Semibold"),
          panel.border = element_blank(), axis.line=element_blank(),
          panel.grid.minor=element_blank(),
          panel.grid.major.y = element_blank(),
          panel.grid.major.x = element_line(size=0.5, colour="grey80"),
          axis.ticks=element_blank(),
          legend.position="bottom", 
          axis.title=element_text(size=rel(0.8), family="Source Sans Pro Semibold"),
          strip.text=element_text(size=rel(1), family="Source Sans Pro Semibold"),
          strip.background=element_rect(fill="#ffffff", colour=NA),
          panel.spacing.y=unit(1.5, "lines"),
          legend.key = element_blank())
  
  ret
}

# Calculate where to put the dotted lines that show up every three entries
x.breaks <- seq(length(tasks$Task) + 0.5 - 3, 0, by=-3)

# Build plot
timeline <- ggplot(tasks.long, aes(x=Task, y=task.date, colour=Project)) + 
  geom_line(size=6) + 
  geom_vline(xintercept=x.breaks, colour="grey80", linetype="dotted") + 
  guides(colour=guide_legend(title=NULL)) +
  labs(x=NULL, y=NULL) + coord_flip() +
  scale_y_date(date_breaks="2 months", labels=date_format("%b ‘%y")) +
  theme_gantt() + theme(axis.text.x=element_text(angle=45, hjust=1))
timeline