df <- data.frame(GDP_Education)

install.packages("zoo")
library(zoo)


df_f <- function(df) {
  for (col in names(df)) {
    missing_indices <- which(is.na(df[[col]]))
    if (length(missing_indices) > 0) {
      non_missing_indices <- which(!is.na(df[[col]]))
      if (length(non_missing_indices) >= 2) {
        non_missing_values <- df[[col]][non_missing_indices]
        interpolated_values <- approx(
          x = non_missing_indices,
          y = non_missing_values,
          xout = missing_indices
        )$y
        df[[col]][missing_indices] <- interpolated_values
      }
    }
  }
  return(df)
}

df <- df_f(df)

mean10 <- mean(df[df$year == '2010', "gdp"])
mean11 <- mean(df[df$year == '2011', "gdp"])
mean12 <- mean(df[df$year == '2012', "gdp"])
mean13 <- mean(df[df$year == '2013', "gdp"])
mean14 <- mean(df[df$year == '2014', "gdp"])
mean15 <- mean(df[df$year == '2015', "gdp"])
mean16 <- mean(df[df$year == '2016', "gdp"])
mean17 <- mean(df[df$year == '2017', "gdp"])
mean18 <- mean(df[df$year == '2018', "gdp"])
mean19 <- mean(df[df$year == '2019', "gdp"])

meanx <- c(mean10, mean11, mean12, mean13, mean14, mean15, mean16,
           mean17, mean18, mean19)
plot(meanx, xlab = "Year", ylab = "Mean GDP", main = "Mean GDP of Countries Each Year")
year_ticks <- c(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
year_labels <- c("2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", 
                 "2018", "2019")
axis(1, at = year_ticks, labels = year_labels)

mean10c <- mean(df[df$year == '2010', "cpi_from_2010"])
mean11c <- mean(df[df$year == '2011', "cpi_from_2010"])
mean12c <- mean(df[df$year == '2012', "cpi_from_2010"])
mean13c <- mean(df[df$year == '2013', "cpi_from_2010"])
mean14c <- mean(df[df$year == '2014', "cpi_from_2010"])
mean15c <- mean(df[df$year == '2015', "cpi_from_2010"])
mean16c <- mean(df[df$year == '2016', "cpi_from_2010"])
mean17c <- mean(df[df$year == '2017', "cpi_from_2010"])
mean18c <- mean(df[df$year == '2018', "cpi_from_2010"])
mean19c <- mean(df[df$year == '2019', "cpi_from_2010"])
meanxc <- c(mean10c, mean11c, mean12c, mean13c, mean14c, mean15c, mean16c, 
            mean17c, mean18c, mean19c)
plot(meanxc, xlab = "Year", ylab = "Mean CPI", main = "Mean CPI of Countries Each Year")
axis(1, at = year_ticks, labels = year_labels)

mean10g <- mean(df[df$year == '2010', "gni"])
mean11g <- mean(df[df$year == '2011', "gni"])
mean12g <- mean(df[df$year == '2012', "gni"])
mean13g <- mean(df[df$year == '2013', "gni"])
mean14g <- mean(df[df$year == '2014', "gni"])
mean15g <- mean(df[df$year == '2015', "gni"])
mean16g <- mean(df[df$year == '2016', "gni"])
mean17g <- mean(df[df$year == '2017', "gni"])
mean18g <- mean(df[df$year == '2018', "gni"])
mean19g <- mean(df[df$year == '2019', "gni"])

meanxg <- c(mean10g, mean11g, mean12g, mean13g, mean14g, mean15g, mean16g,
            mean17g, mean18g, mean19g)
plot(meanxg, xlab="Year", ylab="Mean GNI", main="Mean GNI of Countries Each Year")
axis(1, at = year_ticks, labels = year_labels)

install.packages("datarium")
install.packages("tidyverse")
install.packages("corrplot")
install.packages("rcompanion")
install.packages("dplyr")

library(datarium)
library(tidyverse)
library(corrplot)
library(rcompanion)
library(dplyr)

cor(df$gdp, df$education_expenditure_percent, method = "spearman")
variables_to_compare <- df %>% select(-country, -year, -gdp, -gni, -gdp_per_capita)
round(cor(variables_to_compare), digits = 2)
corrplot(cor(variables_to_compare), method = "number", type = "upper")

install.packages("qqplotr")
install.packages("ggplot2")
install.packages("car")
install.packages("caret")
install.packages("stats")

library(qqplotr)
library(ggplot2)
library(car)
library(caret)
library(stats)


hyp1 <- df[, c("completed_upper_secondary_percent", "gdp_growth_percent")]
check_missing_values1 <- colSums(is.na(hyp1))
check_missing_values1

hyp1$log_second <- log(hyp1$completed_upper_secondary_percent)
hyp1$log_gdp_growth <- log(hyp1$gdp_growth_percent)

hist(hyp1$log_second)
hist(hyp1$log_gdp_growth)

hyp1$sqrt_second <- sqrt(hyp1$completed_upper_secondary_percent)
hyp1$sqrt_gdp_growth <- sqrt(hyp1$gdp_growth_percent)

hyp1$cbrt_second <- hyp1$completed_upper_secondary_percent^(1/3)
hyp1$cbrt_gdp_growth <- hyp1$gdp_growth_percent^(1/3)

hist(hyp1$cbrt_second)
hist(hyp1$cbrt_gdp_growth)

shapiro.test(hyp1$log_second)
shapiro.test(hyp1$log_gdp_growth)

shapiro.test(hyp1$cbrt_second)
shapiro.test(hyp1$cbrt_gdp_growth)


hyp2 <- df[, c("education_expenditure_percent", "unemployment_percent")]
check_missing_values2 <- colSums(is.na(hyp2))
check_missing_values2

hyp2$log_expenditure <- log(hyp2$education_expenditure_percent)
hyp2$log_unemployment <- log(hyp2$unemployment_percent)

hist(hyp2$log_expenditure)
hist(hyp2$log_unemployment)

shapiro.test(hyp2$log_education)
shapiro.test(hyp2$log_unemployment)

hyp2$sqrt_expenditure <- sqrt(hyp2$education_expenditure_percent)
hyp2$sqrt_unemployment <- sqrt(hyp2$unemployment_percent)

shapiro.test(hyp2$sqrt_expenditure)
shapiro.test(hyp2$sqrt_unemployment)

hyp2$cbrt_expenditure <- hyp2$education_expenditure_percent^(1/3)
hyp2$cbrt_unemployment <- hyp2$unemployment_percent^(1/3)

hist(hyp2$cbrt_expenditure)
hist(hyp2$cbrt_unemployment)

shapiro.test(hyp2$cbrt_education)
shapiro.test(hyp2$cbrt_unemployment)

plot(hyp1$cbrt_second, hyp1$cbrt_gdp_growth,
     xlab = "Completed Upper Secondary %",
     ylab = "GDP Growth %",
     main = "Relationship between Completing Upper Secondary and GDP Growth")

cor1 <- cor(hyp1$cbrt_second, hyp1$cbrt_gdp_growth, use = "complete.obs")
cor1

reg1 <- lm(cbrt_second ~ cbrt_gdp_growth, hyp1)
summary.lm(reg1)

hyp1$cbrt_expenditure <- df$education_expenditure_percent^(1/3)
hyp1$cbrt_labor <- df$labor_force_participation_percent^(1/3)
hyp1$cbrt_gdp_pc <- df$gdp_per_capita^(1/3)

model_1 <- lm(cbrt_gdp_growth ~ cbrt_second + cbrt_labor + cbrt_gdp_pc , hyp1)
summary.lm(model_1)

plot(hyp2$cbrt_education, hyp2$cbrt_unemployment,
     xlab = "Expenditure on Education (% of GDP)",
     ylab = "Unemployment (% of Economically Active Population",
     main = "Relationship between Expenditure on Education and Unemployment")

cor2 <- cor(hyp2$cbrt_expenditure, hyp2$cbrt_unemployment)
cor2

reg2 <- lm(cbrt_expenditure ~ cbrt_unemployment, hyp2)
summary.lm(reg2)

hyp2$cbrt_cpi <- df$cpi_from_2010^(1/3)
hyp2$cbrt_gdp_pc <- df$gdp_per_capita^(1/3)

model_2 <- lm(cbrt_unemployment ~ cbrt_expenditure + cbrt_education + cbrt_gdp_pc, hyp2)
summary.lm(model_2)

install.packages("TTR")
install.packages("forecast")

library(TTR)
library(forecast)

hyp1_ts <- ts(hyp1)
plot(hyp1_ts)

hyp1_data <- head(hyp1, 10)
hyp1_ts <- ts(hyp1_data)
plot(hyp1_ts)

arima_model <- auto.arima(hyp1_ts[, 2])
forecast_data <- forecast(arima_model, h=5)
forecast_data
plot(forecast_data)



