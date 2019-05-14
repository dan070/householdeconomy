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
# Test: Try creating sto from a nonexistent database. Should generate error.
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
# Test: Try creating sto from a nonexistant table.
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
# Test: Try writing to a db(file) with no permission to write to.
# /////////////////////////////////////////////////////////////////////
# ## NOT IMPLEMENTED YET.


# /////////////////////////////////////////////////////////////////////
# Test: Try faulty "dbtype" arguments.
# /////////////////////////////////////////////////////////////////////
# ## NOT IMPLEMENTED YET.


# /////////////////////////////////////////////////////////////////////
# Try database variations (if any)...
# /////////////////////////////////////////////////////////////////////
tempdbpath <- list(tempfile())
db <- tempdbpath[[1]]
for(db in tempdbpath){
  
  # /////////////////////////////////////////////////////////////////////
  #
  # Create table with test data
  #
  # /////////////////////////////////////////////////////////////////////
  
  # Create connection object to sqlite.
  con_temp <- DBI::dbConnect(drv = RSQLite::SQLite(), dbname = db)
  #DBI::dbListTables(con_temp)

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
  # Set seed & table size.
  size_n <- 100
  set.seed(seed = 20190427, kind = "default")
  
  # Generate random numbers to df, and ...
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
  
  # ... prep for SQL insert...
  inserts <-
    paste(apply(
      X = randomrows,
      MARGIN = 1,
      FUN = function(x)
        paste("(", paste(x, collapse = ","), ")")
    ), collapse = ",")
  
  # ... and insert into data base table.
  DBI::dbExecute(con_temp, paste("INSERT INTO test1
                                 VALUES", inserts))
  
  # Disconnect db connection.
  DBI::dbDisconnect(con_temp)

  
  
  # /////////////////////////////////////////////////////////////////////
  # Test: Check subset operator. ie. use [,] notation to read from table.
  # /////////////////////////////////////////////////////////////////////
  # Create sto object.
  sto2 <- NULL
  sto2 <- SmallTableObject$new(dbtype = "sqlite", host = db, tablename = "test1")

    
  # Get original table.  
  con_temp <- DBI::dbConnect(drv = RSQLite::SQLite(), dbname = db)
  mytemp <- DBI::dbGetQuery(con_temp, "select * from test1")
  DBI::dbDisconnect(con_temp)
  
  # 

  test_name <- "Subsetting data using vectors length 1."
  test_status <- F
  tictoc::tic(test_name)
  tryCatch({
    
    # Test: Use both col and row subsetting.
    test_name <- "Subsetting with col and row together, ie: sto[19, 25]"
    tictoc::tic(test_name)
    tmp <- sto2[1, 1]
    checkmate::assert_true(!is.null(tmp)) 
    checkmate::assert_true(length(length(tmp)) == 1) 
    checkmate::assert_true(length(tmp) == 1) 
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste("PASS", ":", test_name, ":", secs, " secs" ))
    
    # Test: Use row subsetting.
    test_name <- "Subsetting with row, ie: sto[27, ]"
    tictoc::tic(test_name)
    tmp <- sto2[1, ]
    checkmate::assert_true(!is.null(tmp)) 
    checkmate::assert_true(length(length(tmp)) == 1) 
    checkmate::assert_true(length(tmp) == 4)
    checkmate::assert_true(nrow(tmp) == 1)
    checkmate::assert_true(class(tmp) == "data.frame")
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste("PASS", ":", test_name, ":", secs, " secs" ))
    
    # Test: Use col subsetting.
    test_name <- "Subsetting with col, ie: sto[, 3]"
    tictoc::tic(test_name)
    tmp <- sto2[, 1]
    checkmate::assert_true(!is.null(tmp)) 
    checkmate::assert_true(length(length(tmp)) == 1) 
    checkmate::assert_true(length(tmp) == 102)
    checkmate::assert_true(class(tmp) != "data.frame")
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste("PASS", ":", test_name, ":", secs, " secs" ))

    
    # Test: Use row subsetting with boolean vector.
    test_name <- "Subsetting using boolean vector ie: sto[c(T, T, F....), ]"
    tictoc::tic(test_name)
    temp_booleanvec <- sample(x = c(TRUE, FALSE), size = nrow(sto2[,]), replace = T)
    tmp <- sto2[temp_booleanvec, ]
    checkmate::assert_true(!is.null(tmp)) 
    checkmate::assert_true(length(length(tmp)) == 1) 
    checkmate::assert_true(length(tmp) == 4)
    checkmate::assert_true(nrow(tmp) == sum(temp_booleanvec))
    checkmate::assert_true(class(tmp) == "data.frame")
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste("PASS", ":", test_name, ":", secs, " secs" ))

    
    # Test: Use col subsetting with column name.
    test_name <- "Subsetting columns with name, ie: sto[ , 'a']"
    tictoc::tic(test_name)
    tmp <- sto2[, "a"]
    checkmate::assert_true(!is.null(tmp)) 
    checkmate::assert_true(length(length(tmp)) == 1) 
    checkmate::assert_true(length(tmp) == 102)
    tmp <- sto2[, "d"]
    checkmate::assert_true(!is.null(tmp)) 
    checkmate::assert_true(length(length(tmp)) == 1) 
    checkmate::assert_true(length(tmp) == 102)
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste("PASS", ":", test_name, ":", secs, " secs" ))
    
    # Test: out of range row subsetting.
    test_name <- "Subsetting rows out of range, ie: sto[ 99999999, ]"
    tictoc::tic(test_name)
    tmp <- sto2[99999999, ]
    checkmate::assert_true(!is.null(tmp)) 
    checkmate::assert_true(length(length(tmp)) == 1) 
    checkmate::assert_true(length(tmp) == 4)
    checkmate::assert_true(nrow(tmp) == 1)
    # Not all classes (eg bit64::integer64) returns NA in this setting.
    # So we cannot really test for NA on all columns for "ghost" rows.
    TODO: figure out how ghost rows can be checked. Or if we leave it like that. Should most be NA??
    mytemp[99999999999, ]
    mytemp[200, ]
    str(mytemp)
    mytemp[mytemp$a == 9218868437227407266, ]

    mytemp2 <- mytemp
    
    
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste("PASS", ":", test_name, ":", secs, " secs" ))
    
    
    
    tests_assert$getMessages()
    
    TODO : 
    keep writing tests for subsetting only.
    # TODO: 
    #   write a bunch of tests here, for subsetting.
    # And subvert all tests to force out errors that are common.
    #   1. regular notation on x and y
    #   2. using boolean vector
    #   3. using the set itself sto[sto[, 1]>10000, ]
    #   4. sto[, "a"]
    #   5. out of range testing, should not work, x and y dimension.
    #   6. abuse notation generally
    #   7. run apply functions on data.
    
    # Test passed: TRUE.
    test_name <- "Subsetting data"
    test_status <- T
  }, error = function(e){
    test_name <<- "Subsetting data using vectors length 1, general error." # General error.
    test_status <<- F # Anything could have gone wrong here. 
  }, finally = {
    sto1 <- NULL # Clear object.
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste(ifelse(test_status, "PASS", "FAIL"), ":", test_name, ":", secs, " secs" ))
    
  })
  
  
  test_status <- NULL
  sto2[, ] # Should work.
  
  tests_assert$push(msg = paste(ifelse(test_status, "PASS", "FAIL"), ":", test_name, ":", secs, " secs" ))
  
  
  
  sto2[1, 1] # Should work.
  sto2[, 1] # Should work.
  sto2[-1, ] # Should work.
  sto2[, -1] # Should work.
  sto2[1010101010101, ] # Should kind of work, return NA for all values.
  sto2[, 1010101010101] # Should not work.
  sto2[sto2[, 1] > 10000000, ]
  
  
  # /////////////////////////////////////////////////////////////////////
  # Check updates on each column.
  #
  # Create a sto on table.
  # Change 1 value in each column (add 1, or "a"). Write back each change.
  # Change 1 value to NA in each column, and write back for each change.
  # Set each column to NA, one at a time, and write back.
  # /////////////////////////////////////////////////////////////////////
  

  # Get original table.  
  con_temp <- DBI::dbConnect(drv = RSQLite::SQLite(), dbname = db)
  mytemp <- DBI::dbGetQuery(con_temp, "select * from test1")
  DBI::dbDisconnect(con_temp)
  
  # Change 1 value in each column and write back.  
  sto2[1, 1]
  sto2 <- NULL

  
  # Overload the operators [], as if this was a genuine data frame.
  # Send in a value then modify the underlying data base.
  # Read, then just return the DF.
  # Assign, and update the underlying table.
  
  # Case 1
  tmp <- sto2[1, ]
  sto2[, ] <- tmp
  sto2[, 1]
  
  # Case 2
  sto2[1, 1] <- 100
  
  
  
  sto2[, 1] <- 100
  sto2[1, ] <- 100
  
  class(sto2)
  
  
  # test what happens when abusing the notation
  tmp <- cars
  tmp[1, 1] <- NA
  tmp[, 1] <- "2a"
  lapply(tmp, class)
  
  
  
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



