library(readr)
library("poweRlaw")
library(ggplot2)
X3_1_raw <- read_csv("Projects/CS598-cloud-computing-capstone/question_3/3_1_raw.csv")

View(X3_1_raw)

plot(X3_1_raw[2])

counts <- as.data.frame(X3_1_raw[2])
m_pl = displ$new(as.numeric(counts[1,]))

est = estimate_xmin(m_pl)

print(est)

m_pl$setXmin(est)

est = estimate_xmin(m_bl)


plot(m_pl)
lines(m_pl, col = 2)
lines(m_ln, col = 3)
lines(m_pois, col = 4)

