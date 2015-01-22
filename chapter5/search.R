s <- read.csv('/Users/lee/machine/programming-collective-intelligence-code/chapter5/schedule.txt', header=F)
colnames(s) <- c('from','to','depart','arrive','cost')
s[s$from == 'BOS',]
nrow(s[s$from == 'BOS',])