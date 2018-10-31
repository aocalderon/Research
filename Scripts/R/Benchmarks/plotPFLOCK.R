#!/usr/bin/Rscript

require(ggplot2)
require(stringr)

READ_DATA     = T
SAVE_PDF      = F
SEP           = "###"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = paste0(RESEARCH_HOME, 'Scripts/Python/Tests/Test005.txt')

lines    = readLines(dataFile)
records  = c()
method   = ""
epsilon  = 0
mu       = 0
delta    = 0
time     = 0

run_id  = 0
collect = F
if(READ_DATA){
  for(line in lines){
    #print(line)
    if(grepl("PFLOCK_START", line)){
      collect = T
    }
    if(grepl("PFLOCK_END", line)){
      collect = F
      run_id = run_id + 1
    }
    if(collect){
      params = str_split_fixed(line, " -> ", 2)
      run_timestamp = str_trim(params[1])
      run_line = str_trim(params[2])
      if(run_line != ""){
        row = paste0(run_id,SEP,run_timestamp,SEP, run_line)
        #print(row)
        records = c(records, row)
      }
    }
  }
  data = as.tibble(str_split_fixed(records,SEP, 3))
  runs = filter(data, grepl("spark-submit", V3)) %>% 
    select(V1,V3) %>%
    separate(V3, sep = "--", into = letters[1:11]) %>%
    select(V1, c, e, g) %>%
    separate(c, sep = " ", c(NA, "Epsilon")) %>%
    separate(e, sep = " ", c(NA, "Mu")) %>%
    separate(g, sep = " ", c(NA, "Delta"))
  
  
  #f = f$V3[, c(3,5,7)]
  # names(data) = c("Method", "Epsilon", "Mu", "Delta", "Time")
  # data$Epsilon = as.numeric(as.character(data$Epsilon))
  # data$Mu      = as.numeric(as.character(data$Mu))
  # data$Delta   = as.numeric(as.character(data$Delta))
  # data$Time    = as.numeric(as.character(data$Time))
}

# title = "Execution time by delta"
# g = ggplot(data=data, aes(x=factor(Epsilon), y=Time, fill=Method)) +
#   geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
#   labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) 
# if(SAVE_PDF){
#   ggsave("./MergeLastStagebyDelta.pdf", g)
# } else {
#   plot(g)
# }
