checkF <- function(ID, Stage, TS, Time, Start, End){ 
  check = Start < Time && Time < End
  x <- as_tibble(check) 
}

k = j %>% select(ID, Stage, TS, Time, Start, End) %>% pmap_dfr(checkF)
