# Instructions: 
# Run this script top down to test all functions, objects and other code.
# Errors will be printed out.

source("./src/objects.R")
library(checkmate)
tests_assert <- checkmate::makeAssertCollection()

# ~~~~~~
# Using sto with sqlite...
# ~~~~~~

# /////////////////////////////////////////////////////////////////////
# Try creating sto from a nonexistent database. Should generate error.
# /////////////////////////////////////////////////////////////////////
test_name <- "Create table object from nonexistent db should not be possible."
test_status <- F
tictoc::tic(test_name)
tryCatch({
  sto1 <- NULL
  sto1 <-
    SmallTableObject$new(dbtype = "sqlite",
                         host = tempfile(fileext = ".sqlite"),
                         tablename = "test")
  test_status <- F
}, error = function(e){
  test_status <<- T
}, finally = {
  sto1 <- NULL
  timespent <- tictoc::toc(quiet = T)
  secs <- round(timespent$toc - timespent$tic, 4)
})
tests_assert$push(msg = paste(ifelse(test_status, "PASS", "FAIL"), ":", test_name, ":", secs, " secs" ))



# /////////////////////////////////////////////////////////////////////
# Try creating sto from a nonexistant table.
# /////////////////////////////////////////////////////////////////////
test_name <- "Create table object from nonexistent table, should not be possible."
test_status <- F
tictoc::tic(test_name)
tryCatch({
  sto1 <- NULL
  tmpfile <- tempfile(fileext = ".sqlite")
  tmpconn <- DBI::dbConnect(drv = RSQLite::SQLite(), dbname = tmpfile)
  DBI::dbDisconnect(tmpconn)
  sto1 <-
    SmallTableObject$new(dbtype = "sqlite",
                         host = tmpfile,
                         tablename = "test")
  test_status <- F
}, error = function(e){
  test_status <<- T
}, finally = {
  sto1 <- NULL
  unlink(tmpfile)
  timespent <- tictoc::toc(quiet = T)
  secs <- round(timespent$toc - timespent$tic, 4)
})
tests_assert$push(msg = paste(ifelse(test_status, "PASS", "FAIL"), ":", test_name, ":", secs, " secs" ))


# 
tests_assert$getMessages()


# /////////////////////////////////////////////////////////////////////
# Try writing to a db(file) with no permission to write to.
# /////////////////////////////////////////////////////////////////////
# ## NOT IMPLEMENTED YET.


# /////////////////////////////////////////////////////////////////////
# Try faulty "dbtype" arguments.
# /////////////////////////////////////////////////////////////////////
# ## NOT IMPLEMENTED YET.


# /////////////////////////////////////////////////////////////////////
# Try database variations (if any)...
# /////////////////////////////////////////////////////////////////////
tempdbpath <- list(tempfile())
for(db in tempdbpath){
  
  # /////////////////////////////////////////////////////////////////////
  # Create random table for subsequent tests
  # /////////////////////////////////////////////////////////////////////
  
  # Create connection object to sqlite.
  con_temp <- DBI::dbConnect(drv = RSQLite::SQLite(), dbname = db)
  

  # Create a table with all pertinent datatypes
  #   -Date/time specific class is not an option for sqlite.
  #   -SQLIte has 4 "storage classes" and 1 column can mix different types(!).
  #   -More on data types : http://www.sqlitetutorial.net/sqlite-data-types/
  DBI::dbExecute(con_temp, "CREATE TABLE test1 (a int , b real , c text , d blob )")

  # Insert 100 random rows for each column.
  #   -No missing values.
  #   -Ensure both positive and negative numerics.
  #   -Ensure empty strings, but not NULLs/NA. Ensure åäö nordic characters in strings.
  #   -Ensure dates in the past, and the future. (not applicable here)
  DBI::dbExecute(con_temp, "INSERT INTO test1
                 VALUES(  
                 9223372036854775807, 
                 9.0, 
                 'abcdefghijklmnopqrstuvxyzåäö 1234567890?!', 
                 x'050015a0' ), 
                  ( 
                 -9223372036854775805, 
                 -9.9, 
                 '', 
                 x'0419ffff' )
                 ")
  # Set seed and table size.
  size_n <- 100
  set.seed(seed = 20190427, kind = "default")
  
  # Make random numbers and ...
  randomrows <- data.frame(
    a = sample.int(n = 999999999999, size = size_n) * sample(
      x = c(-1, 1),
      size = size_n,
      replace = T
    ),
    b = runif(
      n = size_n,
      min = -99999999,
      max = 99999999
    ),
    c = replicate(n = size_n, expr = paste("'", paste(
      sample(
        x = c(LETTERS, letters),
        size = 10,
        replace = T
      ), collapse = ""
    ), "'", sep = "")  ),
    d = replicate(n = size_n, expr = paste(
      "x'", paste(sample(
        x = c(0:9, letters[1:6]),
        size = 10,
        replace = T
      ), collapse = ""), "'", sep = ""
    ))
  )
  
  # ... insert into table.
  inserts <-
    paste(apply(
      X = randomrows,
      MARGIN = 1,
      FUN = function(x)
        paste("(", paste(x, collapse = ","), ")")
    ), collapse = ",")
  
  # Insert.
  DBI::dbExecute(con_temp, paste("INSERT INTO test1
                                 VALUES", inserts))
  
  DBI::dbDisconnect(con_temp)
  # /////////////////////////////////////////////////////////////////////
  # Check updates on each column.
  # /////////////////////////////////////////////////////////////////////
  # Create a sto on table.
  # Change 1 value in each column (add 1, or "a"). Write back each change.
  # Change 1 value to NA in each column, and write back for each change.
  # Set each column to NA, one at a time, and write back.
  #
  sto2 <- SmallTableObject$new(dbtype = "sqlite", host = db, tablename = "test1")
  
  tmp <- RSQLite::dbConnect(drv = RSQLite::SQLite(), dbname = db)
  TODO: row 203 , errors:  Error in xi == xj : comparison of these types is not implemented 
  
  
  con_temp
  
  
  
  # Clear the local copy of table and randomise a new, 100 rows for each column.
  # Use set method to write back a new table.
  # Remove 1 row, and write back.
  # Add 1 row, and write back.
  # 
  # Delete one or more rows from the db table.
  # Change 1 value on local table.
  # Write back should now fail.
  # 
  # Unlink the old sto, and create a new sto.
  # Change 1 value on the db table.
  # Write back should now fail.
  # Unlink the sto.
  #
  # Create an empty table on the db.
  # Create a sto on that empty table.
  # Try putting only strings into the local df.
  # Write back should fail.
  
  
  #tmp <- DBI::dbGetQuery(con_temp, "drop table test1")
  tmp <- DBI::dbGetQuery(con_temp, "select * from test1")
  #tmp <- DBI::dbGetQuery(con_temp, "delete from  test1")
  str(tmp)
  tmp$blob
  
  # Disconnect (kills all temporary databases)
  DBI::dbDisconnect(con_temp)
    
}



