# Snabbkoll på siffrorna.


tr1 <- data.frame()
for(f1 in list.files(path = "./data-transactions/database", full.names = TRUE)){
  t1 <- gnumeric::read.gnumeric.sheet(file = f1, head = TRUE, sheet.name = "data")
  tr1 <- rbind(tr1, t1)
}

readLines(f1, 3)



tr1 %>% 
  mutate(transactiondate = as_date(transactiondate)) %>% 
  mutate(month = floor_date(transactiondate, "month")) %>% 
  group_by(category, month) %>% 
  summarise(spend = sum(amount)) %>% 
  arrange(spend)
  
tr1 %>% 
  mutate(transactiondate = as_date(transactiondate)) %>% 
  mutate(month = floor_date(transactiondate, "month")) %>% 
  group_by(category, month) %>% 
  summarise(spend = sum(amount)) %>% 
  
  