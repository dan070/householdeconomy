# functions


unlabeled_getdata <- function(){
  mydb <- DBI::dbConnect(RSQLite::SQLite(), "../data/sqlite-database/econ.sqlite")
  res <- dbGetQuery(mydb, "select * from unlabeled")
  dbDisconnect(mydb)
  return(res)
}

unlabeled_setdata <- function(rhot, sqlite_connection){
  
  # Args: 
  #   rhot                data frame with the same columns as "unlabeled" table.
  #   sqlite_connection   
  
  # When clicked save, we put the data back into unlabeled table.
  # Read the rho-table and update "unlabeled" with the labels.
  # Then read and return.
  
  # Get connection to DB.
  #mydb <- DBI::dbConnect(RSQLite::SQLite(), "../data/sqlite-database/econ.sqlite")
  mydb <- sqlite_connection
  
  
  # Clear the staging table
  dbExecute(mydb, "DELETE FROM unlabeled_update")
  dbExecute(mydb, "DELETE FROM unlabeled_stage")
  
  ## Get data from shiny rhot.
  ##inp <- hot_to_r(input$rhot1)
  
  # Put the current rho table data into update table
  dbWriteTable(mydb, "unlabeled_update", rhot, overwrite = T)
  
  # Join  unlabeled with  update =>  stage.
  # More work than normal , since sqlite has slightly strange 
  # update-conditions when updating from another table.
  dbExecute(mydb, "
            INSERT INTO unlabeled_stage
            SELECT 
            a.transactiontext, 
            a.transactiondate, 
            a.bookingdate, 
            a.amount,
            a.balance,
            COALESCE(b.manual_label, a.manual_label),
            a.predicted_label
            FROM unlabeled AS a
            LEFT JOIN unlabeled_update AS b
            ON 
            a.transactiontext = b.transactiontext
            AND 
            a.transactiondate = b.transactiondate
            AND   
            a.bookingdate = b.bookingdate  
            AND 
            a.amount = b.amount  
            AND 
            a.balance = b.balance 
            ")
  
  # TEST: as many rows in stage as in destination table.
  nrow_stage <- dbGetQuery(mydb, "select count(*) as nrow from unlabeled_stage")
  nrow_dest <-  dbGetQuery(mydb, "select count(*) as nrow from unlabeled")
  assertthat::assert_that(nrow_stage$nrow == nrow_dest$nrow)
  
  # Overwrite the unlabeled
  dbExecute("DELETE FROM unlabeled")
  dbExecute("INSERT INTO unlabeled SELECT * FROM unlabeled_stage")
  
  # Clear the staging table
  dbExecute(mydb, "DELETE FROM unlabeled_update")
  dbExecute(mydb, "DELETE FROM unlabeled_stage")
  
  # Get current unlabeled data
  res <- dbGetQuery(mydb, "select * from unlabeled")
  
  # Disconnect from database.  
  dbDisconnect(mydb)
  
  # Return current unlabeled data
  return(res)
  
}

unlabeled_truncate <- function(sqlite_connection){
  
  # Connect.
  #mydb <- DBI::dbConnect(RSQLite::SQLite(), "../data/sqlite-database/econ.sqlite")
  mydb <- sqlite_connection
  
  # Clear the unlabeled table
  dbExecute(mydb, "DELETE FROM unlabeled")
  
  # Disconnect.
  dbDisconnect(mydb)
  
}

labeled_setdata <- function(sqlite_connection){
  
  # Move from unlabeled data set to labeled data set.
  
  # Connect.
  #mydb <- DBI::dbConnect(RSQLite::SQLite(), "../data/sqlite-database/econ.sqlite")
  mydb <- sqlite_connection
  
  # Assume data is in unlabeled.
  # TEST: there is more than 0 rows in unlabeled.
  nrow_unlab <- dbGetQuery(mydb, "select count(*) as nrow from unlabeled")
  assertthat::assert_that(nrow_unlab$nrow > 0)
  
  # TEST: all rows have either manual labels or predicted labels.
  nrow_nolabs <- dbGetQuery(mydb, "
                            select  
                            count(*) as nrow 
                            from unlabeled 
                            where 
                            manual_label = '' 
                            and  
                            predicted_label = '' ")
  assertthat::assert_that(nrow_nolabs == 0)  
  
  # Clear staging table.
  dbExecute(mydb, "
            DELETE FROM labeled_staging  ")
  
  # Move into labeled staging table.
  dbExecute(mydb, "
            INSERT INTO labeled_staging 
            SELECT 
            transactiontext, 
            transactiondate, 
            bookingdate, 
            amount,
            balance,
            COALESCE(manual_label, predicted_label) AS label
            FROM 
            unlabeled
            ")
  
  # Get all table counts for later testing.
  nrow_unlab <- dbGetQuery(mydb, "select count(*) as nrow from unlabeled")
  nrow_labstage <- dbGetQuery(mydb, "select count(*) as nrow from labeled_stage")
  nrow_lab_before <- dbGetQuery(mydb, "select count(*) as nrow from labeled")
  
  # TEST: same number of rows upserted.
  assertthat::assert_that(nrow_unlab$nrow == nrow_labstage$nrow)
  
  # Insert into labeled set.
  dbExecute(mydb, "
            INSERT INTO labeled 
            SELECT 
            transactiontext, 
            transactiondate, 
            bookingdate, 
            amount,
            balance,
            label
            FROM 
            unlabeled
            ")
  
  # Check rows after insert.
  nrow_lab_after <- dbGetQuery(mydb, "select count(*) as nrow from labeled")
  
  # TEST: we should grow with same number of rows.
  assertthat::assert_that((nrow_lab_after$nrow - nrow_lab_before$nrow) == nrow_unlab$nrow)
  
  
}

