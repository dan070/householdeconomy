# etl-script to read in files


# Read files.
# Check that we have not already the dates in the database.
# The first date in the file is the one we use.
# Store raw in sqlite, "new_data".

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Copy files from online, my salary account ending in 2122.
# Read them in R.
# Ensure encoding is correct.
# Move them into sql-lite database.
# Move the file to processed folder.
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(dplyr)
library(stringr)
library(DBI)

# List files
flist <- list.files(path = "./data/", pattern = ".csv", full.names = T)

# Check that sqlite is up and runnning.
# dbExecute(mydb, "drop table newdata")
# dbExecute(mydb, "drop table unlabeled")
# 

# Get connection to database
mydb <- dbConnect(drv = RSQLite::SQLite(), "./data/sqlite-database/econ.sqlite")

dbExecute(mydb, "create table if not exists newdata (
             transactiontext TEXT, 
          transactiondate TEXT NOT NULL, 
          bookingdate TEXT NOT NULL, 
          amount REAL NOT NULL, 
          balance REAL NOT NULL
)")
dbExecute(mydb, "create table if not exists unlabeled (
             transactiontext TEXT, 
          transactiondate TEXT NOT NULL, 
          bookingdate TEXT NOT NULL, 
          amount REAL NOT NULL, 
          balance REAL NOT NULL,
          manual_label TEXT,
          predicted_label TEXT
)")



# Process one by one.
# For now, we only to 1 file.
for(fi in flist[1]){

  # Read file
  dat1 <- read.csv2(fi, fileEncoding = "UTF-8", dec = ",",  header = F, stringsAsFactors = F, 
                    col.names = c("transactiontext", "transactiondate", "bookingdate", "amount", "balance"))

  # Make numeric of amount  
  dat1$amount %<>% 
    stringr::str_replace_all(pattern = ",", replacement = ".")   %>% 
    stringr::str_replace_all(pattern = " ", replacement = "")   %>% 
    as.numeric() 
  
  # Make numeric of balance  
  dat1$balance %<>%
  stringr::str_replace_all(pattern = ",", replacement = ".")   %>% 
    stringr::str_replace_all(pattern = " ", replacement = "")   %>% 
    as.numeric() 
  
  # Make date of the dates
  dat1$transactiondate %>% lubridate::as_date() -> dat1$transactiondate
  dat1$bookingdate %>% lubridate::as_date() -> dat1$bookingdate
  


  # TEST: We should have only complete cases, else we have error.
  assertthat::assert_that(all(complete.cases(dat1)))
  # TEST: We should have certain classes
  stopifnot(all(sapply(dat1, class) == c("character", "Date", "Date", "numeric", "numeric")))
  # TEST: We should have at least 10 transactions
  assertthat::assert_that(nrow(dat1) > 10)
  
  # Move to sqlite...

  # Put this table into the new data table.
  dbExecute(mydb, "delete from  newdata")
  dbWriteTable(mydb, "newdata", dat1, overwrite = F, append = T)
  
  # TEST: Check that all rows was transferred.
  rowcnt <- dbGetQuery(mydb, "select count(*) as cnt from newdata")
  assertthat::assert_that(rowcnt$cnt == nrow(dat1))
  
  # Move file to transferred folder.
  file_move_success <- file.copy(from = fi, to = "./data/processed-files/", overwrite = F)
  
  # TEST: copy did work.
  assertthat::assert_that(file_move_success == T)
  
  # Remove the original file
  file_delete_success <- file.remove(fi)
  
  # TEST: delete did work.
  assertthat::assert_that(file_delete_success == T)
  
  # Get numbers from DB "unlabeled" 
  test_unlabeled_count <- dbGetQuery(mydb, "select count(*) as cnt from unlabeled")
  
  # Move the new data to table "unlabeled" for later labeling.
  test_inserted_rows <- dbExecute(mydb, "insert into unlabeled select *, '', '' from newdata")
  
  # Get numbers from unlabeled
  test_unlabeled_count_afterinsert <- dbGetQuery(mydb, "select count(*) as cnt from unlabeled")
  
  # TEST: 
  assertthat::assert_that(test_unlabeled_count_afterinsert - test_unlabeled_count == nrow(dat1))
  
}


