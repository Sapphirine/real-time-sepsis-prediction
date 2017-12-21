install.packages(ggplot2)
install.packages(plotly)
install.packages(dplyr)
library(ggplot2)
library(plotly)
library(dplyr)
options(scipen=999)

# 
predictions = read.csv("predictions.csv")
predictions$probability <- as.character(predictions$probability)
predictions$probability <- gsub("\\]", "", 
                                gsub(",", "", 
                                     sub("([//[0-9.]+)", "", predictions$probability)))
predictions$probability <- as.numeric(predictions$probability)
predictions$log_probability <-log(predictions$probability)
predictions$risk <- cut(predictions$probability, breaks = c(0,.30,.70,.95,1),
                        labels = c("Lowest Risk", "Low Risk", "Medium Risk", "High Risk"))

predictions_risk <- predictions %>% subset(probability > 0.25) %>%
  select(X, probability, risk)
risk_sample <- sample_n(predictions_risk, 50) 

risk_sample <- sample_n(read.csv("curated_patients.csv")[1:500,], 50) %>%
  mutate(PatientID = X,
         risk_factor = factor,
         Risk = risk)

ggplot(risk_sample, aes(x=probability, y=probability, fill=Risk,
                             text = paste("PatientID:", X,"<br> Risk Factor:", risk_factor))) +
  geom_jitter(alpha = (1/2), size=3) + scale_x_log10()  
ggplotly(p)
