
# Mapp: data-transactions/database
#  innehåller kategoriserade bank-utdrag.
#  Detta skript skapar 1 fil per månad per konto.
#  Hämta data manuellt från Swedbank.

library(dplyr)

# Ta en lång transak lista från banken.
# Splutta i 1 df per månad.
helper_split_swedbank <- function(df1){
  return(
    f1 %>% 
      mutate(transactiondate = as_date(transactiondate)) %>% 
      mutate(d2 = floor_date(transactiondate, "month")) %>% 
      filter(d2 >= '2019-05-01', d2 < '2019-08-01') %>% 
      mutate(amount = str_replace_all(amount, " ", "")) %>% 
      mutate(amount = str_replace_all(amount, ",", ".")) %>% 
      mutate(amount = as.numeric(amount)) %>% 
      mutate(balance = str_replace_all(balance, " ", "")) %>% 
      mutate(balance = str_replace_all(balance, ",", ".")) %>% 
      mutate(balance = as.numeric(balance)) %>% 
      split(f = factor(.$d2))
  )
}

#-------------
# Dan Hushållskonto 
#-------------
f1 <- read.csv2(
  file = "./data-transactions/dan-hushållskonto-brutto.csv", header = FALSE)
names(f1) <- c("transactions", "transactiondate", "accountingdate", "amount", "balance")

spl1 <- helper_split_swedbank(f1)
for(fl in names(spl1)){
  write.csv(x = spl1[[fl]] %>% select(-d2), 
            file = str_glue("./data-transactions/database/dan-hushållskonto-{fl}-uncategorised.csv"))
}

#-------------
# Dan lönekonto
#-------------
f1 <- read.csv2(
  file = "./data-transactions/dan-lönekonto-brutto.csv", header = FALSE)
names(f1) <- c("transactions", "transactiondate", "accountingdate", "amount", "balance")

# split
d1 <- helper_split_swedbank(f1)

for(fl in names(d1)){
  write.csv(x = d1[[fl]] %>% select(-d2), 
            file = str_glue("./data-transactions/database/dan-lönekonto-{fl}-uncategorised.csv"))
}



readLines(con = "./data-transactions/database/dan-hushållskonto-2019-05-01.csv", 2)

readLines(con = "./data-transactions/dan-hushållskonto-brutto.csv", 2)






