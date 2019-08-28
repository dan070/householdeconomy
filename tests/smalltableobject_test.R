# Instructions: 
# Run this script top down to test all functions, objects and other code.
# Errors will be printed out.
 
source("./src/objects.R")
library(checkmate)
library(crayon)
library(glue)
tests_assert <- checkmate::makeAssertCollection()


# /////////////////////////////////////////////////////////////////////
# Func: Create data for specific database, given a connection.
# /////////////////////////////////////////////////////////////////////
create_test_data <- function(conn, dbtype = "sqlite", n = 100, seed = 20190427){
  checkmate::assert_choice(dbtype, choices = c("sqlite"))
  checkmate::assert_true(DBI::dbIsValid(conn))
  
  if(dbtype == "sqlite"){
    
    # Create connection object to sqlite.
    con_temp <- conn
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
    size_n <- n
    set.seed(seed = seed, kind = "default")
    
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
    
    
    
  }
  
  return(T)
}


# /////////////////////////////////////////////////////////////////////
# Func: Get values from database simpler
# /////////////////////////////////////////////////////////////////////

get_values_from_database <- function(con, where = list()){
  
  if(length(where) == 0) {out <- ""} else {
    out <- where %>% 
      purrr::imap(.f = function(x, y){
        if(class(x) == "character"){
          paste(y, "IN", paste("(", paste(paste("'", x, "'", sep = ""), collapse = ","), ")"))
        } else {
          paste(y, "IN", paste("(", paste(x, collapse = ","), ")") )
        }
      }) %>% purrr::reduce(.f = function(x, y){paste(x, "AND", y)})
    out <- " WHERE " %+% out
  }
  
  
  #return()
  stmnt <- glue("SELECT * FROM test1 {out}")
  print(stmnt)
  res <- DBI::dbGetQuery(conn = con, statement = stmnt)
  DBI::dbDisconnect(con) # Kill connection.
  return(res)
}



# /////////////////////////////////////////////////////////////////////
# Func: Give me a quick random data STO-object
# /////////////////////////////////////////////////////////////////////

get_sto <- function(n = 100, seed = 20202020){
  tryCatch({
    # Create data and an object.
    tf <- tempfile()
    create_test_data(conn = DBI::dbConnect(drv = RSQLite::SQLite(), dbname = tf), n = n, seed = seed)
    sto2 <- NULL
    sto2 <- SmallTableObject$new(dbtype = "sqlite", host = tf, tablename = "test1")
    return(sto2)    
  }, error = function(e){
    stop("Couldnt create new object.")
  })#end trycatch
}  


# /////////////////////////////////////////////////////////////////////
# Func: Compare 2 data frames
# /////////////////////////////////////////////////////////////////////
compare_dfs <- function(...){
  tryCatch({
    args <- list(...)
    if(length(args) < 2) stop("Not at least 2 dfs input to function compare_dfs.")
    #args %>% str()

    tst <- args %>% purrr::map(checkmate::test_data_frame) %>% 
      purrr::reduce(all)
    if(!tst) return(FALSE)
    
    cmb <- args %>% 
      purrr::map(.f = function(x){
        x %>% 
          purrr::map(paste) %>% 
          purrr::map(sort) %>% 
          purrr::map(digest::digest) %>% 
          purrr::reduce(c) %>% 
          sort %>% 
          paste(collapse = "") %>% 
          digest::digest(.)
      }) %>% 
      purrr::reduce(c) %>% 
      expand.grid(., .) %>% 
      purrr::map(paste) 
    
    
      return(purrr::map2(.x = cmb$Var1, .y = cmb$Var2, .f=function(x, y)x==y) %>% 
        purrr::reduce(all))
  }, error = function(e){return(FALSE)})
}



# ~~~~~~
# Database backend : SQLITE
# ~~~~~~

# /////////////////////////////////////////////////////////////////////
# TEST: Try creating sto from a nonexistent database. Should generate error.
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
tests_assert$push(msg = paste(ifelse(test_status, green("PASS"), red("FAIL")), ":", test_name, ":", secs, " secs" ))



# /////////////////////////////////////////////////////////////////////
# TEST: Try creating sto from a nonexistant table.
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
tests_assert$push(msg = paste(ifelse(test_status, green("PASS"), red("FAIL")), ":", test_name, ":", secs, " secs" ))





# /////////////////////////////////////////////////////////////////////
# TEST: Try writing to a db(file) with no permission to write to.
# /////////////////////////////////////////////////////////////////////
# ## NOT IMPLEMENTED YET.


# /////////////////////////////////////////////////////////////////////
# TEST: Try faulty "dbtype" arguments.
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
  create_test_data(conn = DBI::dbConnect(drv = RSQLite::SQLite(), dbname = db))
  

  
  
  # /////////////////////////////////////////////////////////////////////
  # Create sto object.
  # /////////////////////////////////////////////////////////////////////

  # Create object from sqlite.
  sto2 <- NULL
  sto2 <- SmallTableObject$new(dbtype = "sqlite", host = db, tablename = "test1")

    
  # Get original table.  
  con_temp <- DBI::dbConnect(drv = RSQLite::SQLite(), dbname = db)
  mytemp <- DBI::dbGetQuery(con_temp, "select * from test1")
  DBI::dbDisconnect(con_temp)
  
  # 

  # /////////////////////////////////////////////////////////////////////
  # TEST: Check subset operator. 
  # ie. use [,] notation to read from sto.
  # /////////////////////////////////////////////////////////////////////
  test_name <- "Subsetting data using vectors length 1."
  test_status <- F
  tictoc::tic(test_name)
  tryCatch({
    
    # TEST: Use both col and row subsetting.
    test_name <- "Subsetting with col and row together, ie: sto[19, 25]"
    tictoc::tic(test_name)
    tmp <- sto2[1, 1]
    checkmate::assert_true(!is.null(tmp)) 
    checkmate::assert_true(length(length(tmp)) == 1) 
    checkmate::assert_true(length(tmp) == 1) 
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste(green("PASS"), ":", test_name, ":", secs, " secs" ))
    
    # TEST: Use row subsetting.
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
    tests_assert$push(msg = paste(green("PASS"), ":", test_name, ":", secs, " secs" ))
    
    # TEST: Use col subsetting.
    test_name <- "Subsetting with col, ie: sto[, 3]"
    tictoc::tic(test_name)
    tmp <- sto2[, 1]
    checkmate::assert_true(!is.null(tmp)) 
    checkmate::assert_true(length(length(tmp)) == 1) 
    checkmate::assert_true(length(tmp) == 102)
    checkmate::assert_true(class(tmp) != "data.frame")
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste(green("PASS"), ":", test_name, ":", secs, " secs" ))

    
    # TEST: Use row subsetting with boolean vector.
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
    tests_assert$push(msg = paste(green("PASS"), ":", test_name, ":", secs, " secs" ))

    
    # TEST: Use col subsetting with column name.
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
    tests_assert$push(msg = paste(green("PASS"), ":", test_name, ":", secs, " secs" ))
    
    # TEST: out of range row subsetting.
    test_name <- "Subsetting rows out of range, ie: sto[ 99999999, ]"
    tictoc::tic(test_name)
    tmp <- sto2[99999999, ]
    checkmate::assert_true(!is.null(tmp)) 
    checkmate::assert_true(length(length(tmp)) == 1) 
    checkmate::assert_true(length(tmp) == 4)
    checkmate::assert_true(nrow(tmp) == 1)
    # Not all classes (eg bit64::integer64) returns NA in this setting.
    # So we cannot really test for NA on all columns for "ghost" rows.
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste(green("PASS"), ":", test_name, ":", secs, " secs" ))
    
    # TEST: negative row subsetting 
    test_name <- "Subsetting negative rows ie: sto[ -1, ]"
    tictoc::tic(test_name)
    tmp <- sto2[-c(1:100), ]
    checkmate::assert_true(!is.null(tmp)) 
    checkmate::assert_true(length(length(tmp)) == 1) 
    checkmate::assert_true(length(tmp) == 4)
    checkmate::assert_true(nrow(tmp) == 2)
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste(green("PASS"), ":", test_name, ":", secs, " secs" ))
    
    
    # TEST: negative col subsetting 
    test_name <- "Subsetting negative cols ie: sto[ , -1]"
    tictoc::tic(test_name)
    tmp <- sto2[, -c(1:2)]
    checkmate::assert_true(!is.null(tmp)) 
    checkmate::assert_true(length(length(tmp)) == 1) 
    checkmate::assert_true(length(tmp) == 2)
    checkmate::assert_true(nrow(tmp) == 102)
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste(green("PASS"), ":", test_name, ":", secs, " secs" ))
    
    # TEST: negative row and col subsetting
    test_name <- "Subsetting negative rows and cols ie: sto[ -1, -1]"
    tictoc::tic(test_name)
    tmp <- sto2[-c(1:100), -c(1:2)]
    checkmate::assert_true(!is.null(tmp)) 
    checkmate::assert_true(length(length(tmp)) == 1) 
    checkmate::assert_true(length(tmp) == 2)
    checkmate::assert_true(nrow(tmp) == 2)
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste(green("PASS"), ":", test_name, ":", secs, " secs" ))
    
    # TEST: select all the data
    test_name <- "Subsetting total data frame ie: sto[ , ]"
    tictoc::tic(test_name)
    tmp <- sto2[, ]
    checkmate::assert_true(!is.null(tmp)) 
    checkmate::assert_true(length(length(tmp)) == 1) 
    checkmate::assert_true(length(tmp) == 4)
    checkmate::assert_true(nrow(tmp) == 102)
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste(green("PASS"), ":", test_name, ":", secs, " secs" ))
    
    # TEST: select something outside columns sto[, "nonexistingcolumn"]
    test_name <-
      "Subsetting outside data frame ie: sto[ , 'nonexistingcolumn']"
    tictoc::tic(test_name)
    tryCatch(
      expr = {
        tmp <- sto2[, "danistheman"]
        timespent <- tictoc::toc(quiet = T)
        secs <- round(timespent$toc - timespent$tic, 4)
        tests_assert$push(msg = paste(red("FAIL"), ":", test_name, ":", secs, " secs"))
        stop()
      }
      ,
      error = function(e) {
        timespent <- tictoc::toc(quiet = T)
        secs <- round(timespent$toc - timespent$tic, 4)
        tests_assert$push(msg = paste(green("PASS"), ":", test_name, ":", secs, " secs"))
      }
    )
    
    # TEST: select  sto[boolean2dimensionalmatrix]
    # TODO. Not implemented yet.
         
    # TEST: select  sto[-9999, ]
    test_name <- "Subsetting outside data frame ie: sto[-99999999 , ]"
    tictoc::tic(test_name)
    checkmate::assert_set_equal(dim(sto2[-99999, ]), dim(sto2[]), add = tests_assert)
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste(green("PASS"), ":", test_name, ":", secs, " secs" ))
    
    # TEST: select sto[NULL, NULL]
    # NB: At the moment I'm unable to implement this behaviour!
    # NB: Supposed to give an empty data frame back. 
    # > cars[NULL, NULL]
    # data frame with 0 columns and 0 rows
    
    
    # TEST: select sto[NA, NA]
    test_name <-
      "Subsetting with NA ie: sto[NA , NA]"
    tictoc::tic(test_name)
    tryCatch(
      expr = {
        tmp <- sto2[NA, NA]
        timespent <- tictoc::toc(quiet = T)
        secs <- round(timespent$toc - timespent$tic, 4)
        tests_assert$push(msg = paste(red("FAIL"), ":", test_name, ":", secs, " secs"))
        stop()
      }
      ,
      error = function(e) {
        timespent <- tictoc::toc(quiet = T)
        secs <- round(timespent$toc - timespent$tic, 4)
        tests_assert$push(msg = paste(green("PASS"), ":", test_name, ":", secs, " secs"))
      }
    )

        
    # TEST: run apply :    lapply(sto2[, ], class)
    test_name <-
      "Subsetting with NA ie: sto[NA , NA]"
    tictoc::tic(test_name)
    checkmate::assert_set_equal(sapply(mytemp, class)
                                ,   sapply(sto2[], class)
                                , add = tests_assert)
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste(green("PASS"), ":", test_name, ":", secs, " secs"))
    
    # Test passed TRUE.
    test_name <- "Subsetting data tests all of them."
    test_status <- T
  }, error = function(e){
    ##test_name <<- "Subsetting data using vectors length 1, general error." # General error.
    test_status <<- F # Anything could have gone wrong here. 
  }, finally = {
    sto1 <- NULL # Clear object.
    timespent <- tictoc::toc(quiet = T)
    secs <- round(timespent$toc - timespent$tic, 4)
    tests_assert$push(msg = paste(ifelse(test_status, green("PASS"), red$bold("FAIL")), ":", test_name, ":", secs, " secs" ))
  })
  
  

    
  
  
  # /////////////////////////////////////////////////////////////////////
  # Check updates on each column.
  #
  # Create a sto on table. Done: use above code for that.
  # Change 1 value in each column (add 1, or "a"). Write back each change.
  # Change 1 value to NA in each column, and write back for each change.
  # Set each column to NA, one at a time, and write back.
  # /////////////////////////////////////////////////////////////////////

  
  
  
  # TEST: Pin one unique row. Update 1 cell. Compare to data base.  
  sto2 <- get_sto(n = 10, seed = sample.int(10000000, 1))
  test_name <- 
    "Assign 1 cell ie sto[1 , 1] <- 1"
  tictoc::tic(test_name)
  # Assign.
  tmprow <- sample(nrow(sto2[]), 1)
  tmp <- sto2[]
  tmp[tmprow, 1] <- 0
  sto2[tmprow, 1] <- 0
  # Get Database value.
  res <- get_values_from_database(con = DBI::dbConnect(drv = RSQLite::SQLite(), dbname = sto2$get_host), 
                                  where = list())
  
  # Compare.
  if(checkmate::test_true(compare_dfs(sto2[], res, tmp))){
    tests_assert$push(green("PASS: ") %+% test_name )
  } else {
    tests_assert$push(red("FAIL: ") %+% test_name )
  }
  tictoc::toc()
  

  # TEST: Pin 10 unique rows. Update 1 cell. Compare to data base.  
  sto2 <- get_sto(n = 10, seed = sample.int(10000000, 1))
  test_name <-
    "Assign 10 cells ie sto[1:10 , 1] <- 1"
  tictoc::tic(test_name)
  # Assign.
  tmprow <- sample(nrow(sto2[]), 10)
  tmp <- sto2[]
  tmp[tmprow, 1] <- 0
  sto2[tmprow, 1] <- 0
  # Get Database value.
  res <- get_values_from_database(con = DBI::dbConnect(drv = RSQLite::SQLite(), dbname = sto2$get_host), 
                                  where = list())

  # Compare.
  if(checkmate::test_true(compare_dfs(sto2[], res, tmp))){
    tests_assert$push(green("PASS: ") %+% test_name )
  } else {
    tests_assert$push(red("FAIL: ") %+% test_name )
  }
  tictoc::toc()
  
  
  # TEST: Pin 10 unique rows. Update all cells to NA. Compare to data base.
  sto2 <- get_sto(n = 10, seed = sample.int(10000000, 1))
  test_name <-
    "Assign 10 cells ie sto[1:10 , ] <- NA"
  tictoc::tic(test_name)
  tmprow <- sample(nrow(sto2[]), 10)
  tmp <- sto2[]
  tmp[tmprow, 1] <- NA
  sto2[tmprow, 1] <- NA
  # Get Database value.
  res <- get_values_from_database(con = DBI::dbConnect(drv = RSQLite::SQLite(), dbname = sto2$get_host), 
                                  where = list())
  
  # Compare.
  if(checkmate::test_true(compare_dfs(sto2[], res, tmp))){
    tests_assert$push(green("PASS: ") %+% test_name )
  } else {
    tests_assert$push(red("FAIL: ") %+% test_name )
  }
  tictoc::toc()
  
  
  # TEST: Pin 10 unique rows. Delete them. Compare to data base.
  sto2 <- get_sto(n = 10, seed = sample.int(10000000, 1))
  test_name <-
    "Delete 10 rows ie sto[1:10 , ] "
  tictoc::tic(test_name)
  tmprow <- sample(nrow(sto2[]), 10)
  tmp <- sto2[-tmprow, ]
  sto2[,] <- tmp

  # Get Database value.
  res <- get_values_from_database(con = DBI::dbConnect(drv = RSQLite::SQLite(), dbname = sto2$get_host), 
                                  where = list())
  
  # Compare.
  if(checkmate::test_true(compare_dfs(sto2[], res, tmp))){
    tests_assert$push(green("PASS: ") %+% test_name )
  } else {
    tests_assert$push(red("FAIL: ") %+% test_name )
  }
  tictoc::toc()
  
  
  # TEST: Use boolean and named subsetting of columns. Update. Compare to data base.
  sto2 <- get_sto(n = 10, seed = sample.int(10000000, 1))
  test_name <-
    "Assign using bool and named subsetting. sto[ bool , 'a']  <- 1"
  tictoc::tic(test_name)
  tmprow <- sto2[, 'a'] > median(sto2[, 1])
  sto2[tmprow, 'a'] <- 0
  # Get Database value.
  res <- get_values_from_database(con = DBI::dbConnect(drv = RSQLite::SQLite(), dbname = sto2$get_host), 
                                  where = list())
  
  # Compare.
  if(checkmate::test_true(compare_dfs(sto2[], res))){
    tests_assert$push(green("PASS: ") %+% test_name )
  } else {
    tests_assert$push(red("FAIL: ") %+% test_name )
  }
  tictoc::toc()
  

  # TEST: Update each full column with new values = x4. Compare to data base.
  sto2 <- get_sto(n = 10, seed = sample.int(10000000, 1))
  test_name <-
    "Assign column 'a' sto[, 'a']  <- val"
  tictoc::tic(test_name)
  tmp <- sto2[]
  tmp2 <- sample(sto2[1:5, 'a'], nrow(sto2[,]), replace = T)
  tmp[, 'a'] <- tmp2
  sto2[, 'a'] <- tmp2
  # Get Database value.
  res <- get_values_from_database(con = DBI::dbConnect(drv = RSQLite::SQLite(), dbname = sto2$get_host), 
                                  where = list())
  
  # Compare.
  if(checkmate::test_true(compare_dfs(sto2[], res, tmp))){
    tests_assert$push(green("PASS: ") %+% test_name )
  } else {
    tests_assert$push(red("FAIL: ") %+% test_name )
  }
  tictoc::toc()
  
    
  # TEST: Update a whole row with new values = x4. Compare to data base.
  sto2 <- get_sto(n = 10, seed = sample.int(10000000, 1))
  test_name <-
    "Assign row with new values,  sto[1, ]  <- c(1, 2, 3, 4)"
  tictoc::tic(test_name)
  tmp <- sto2[]
  tmp2 <- sample.int(nrow(sto2[]), 2)
  tmp[tmp2[1], ] <- tmp[tmp2[2], ] # Need to see how a local copy would behave.
  sto2[tmp2[1], ] <- sto2[tmp2[2], ] # This shows how the sto object behaves.
  

  # Get Database value.
  res <- get_values_from_database(con = DBI::dbConnect(drv = RSQLite::SQLite(), dbname = sto2$get_host), 
                                  where = list())
  
  # Compare.
  if(checkmate::test_true(compare_dfs(sto2[], res, tmp))){
    tests_assert$push(green("PASS: ") %+% test_name )
  } else {
    tests_assert$push(red("FAIL: ") %+% test_name )
  }
  tictoc::toc()
  
  
  
  # TEST: Try updating 1 cell in each column, with wrong type. Compare to data base.
  sto2 <- get_sto(n = 10, seed = sample.int(10000000, 1))
  test_name <-
    "Assign 1 cell with wrong type,  sto[1, 1]  <- wrong-type-var "
  tictoc::tic(test_name)
  
  tmp[1, 1] <- "hello"
  sto2[1, 1] <- "hello" # should be NA when S3 class.
  # Get Database value.
  res <- get_values_from_database(con = DBI::dbConnect(drv = RSQLite::SQLite(), dbname = sto2$get_host), 
                                  where = list())
  
  # Compare.
  if(checkmate::test_true(compare_dfs(sto2[], res, tmp))){
    tests_assert$push(green("PASS: ") %+% test_name )
  } else {
    tests_assert$push(red("FAIL: ") %+% test_name )
  }
  tictoc::toc()
  

  
  # TEST: Try updating 1 cell in each column, with wrong type. Compare to data base.
  sto2 <- get_sto(n = 10, seed = sample.int(10000000, 1))
  test_name <-
    "Assign 1 cell with wrong type,  sto[1, 1]  <- wrong-type-var "
  tictoc::tic(test_name)
  
  tmp[1, 2] <- "hello"
  
  tryCatch({
    sto2[1, 2] <- "hello" # should throw error, atomic value.
    tests_assert$push(red("FAIL: no error generated") %+% test_name )
  }, error = function(e){
    tests_assert$push(green("PASS: error generated ") %+% test_name )
  }
  )

  # Get Database value.
  res <- get_values_from_database(con = DBI::dbConnect(drv = RSQLite::SQLite(), dbname = sto2$get_host), 
                                  where = list())
  
  # Compare.
  if(checkmate::test_true(compare_dfs(sto2[], res))){
    tests_assert$push(green("PASS: data base intact") %+% test_name )
  } else {
    tests_assert$push(red("FAIL: change to local data made") %+% test_name )
  }
  tictoc::toc()
  
  
  
  # TEST: Try replacing 1 column with wrong type. Compare to data base.
  sto2 <- get_sto(n = 10, seed = sample.int(10000000, 1))
  test_name <-
    "Assign 1 column with wrong type,  sto[, 1]  <- wrong-type-var "
  tictoc::tic(test_name)

  tmp <- sto2[]
  tmp[, 1] <- rep("hello", nrow(sto2[]))  
  

  tryCatch({
    sto2[, 1] <- rep("hello", nrow(sto2[]))  
    tests_assert$push(red("FAIL: no error generated") %+% test_name )
  }, error = function(e){
    tests_assert$push(green("PASS: error generated ") %+% test_name )
  }
  )
  tictoc::toc()
  
  
  # TEST: Try NULLing entire frame. Compare to data base.
  sto2 <- get_sto(n = 10, seed = sample.int(10000000, 1))
  test_name <-
    "Assign NULL to entire frame,  sto[, ]  <- NULL "
  tictoc::tic(test_name)
  
  tryCatch({
    sto2[, 1] <- NULL
    tests_assert$push(red("FAIL: sto2[, 1] <- NULL") %+% test_name )
  }, error = function(e){
    tests_assert$push(green("PASS: sto2[, 1] <- NULL") %+% test_name )
  })
  
  tryCatch({
    sto2[1, ] <- NULL
    tests_assert$push(red("FAIL: sto2[1, ] <- NULL") %+% test_name )
  }, error = function(e){
    tests_assert$push(green("PASS: sto2[1, ] <- NULL") %+% test_name )
  })
  
  tryCatch({
    sto2[, ] <- NULL
    tests_assert$push(red("FAIL: sto2[, ] <- NULL") %+% test_name )
  }, error = function(e){
    tests_assert$push(green("PASS: sto2[, ] <- NULL") %+% test_name )
  })

  tryCatch({
    sto2[1,1] <- NULL
    tests_assert$push(red("FAIL: sto2[1, 1] <- NULL") %+% test_name )
  }, error = function(e){
    tests_assert$push(green("PASS: sto2[1, 1] <- NULL") %+% test_name )
  })
  
  tictoc::toc()
  
  

    
    
  
  
  
  
  # TEST: clear the local copy of table and randomise a new, 100 rows for each column.
  sto2 <- get_sto(n = 100, seed = sample.int(10000000, 1))
  test_name <-
    "Assign 300 new rows,  sto[, ]  <- completelynewdata "
  tictoc::tic(test_name)
  
  tmp <- sample_n(sto2[], 300, replace = T)
  sto2[] <- tmp
  
  # Get Database value.
  res <- get_values_from_database(con = DBI::dbConnect(drv = RSQLite::SQLite(), dbname = sto2$get_host), 
                                  where = list())
  
  # Compare.
  if(checkmate::test_true(compare_dfs(sto2[], res, tmp))){
    tests_assert$push(green("PASS: data base intact") %+% test_name )
  } else {
    tests_assert$push(red("FAIL: change to local data made") %+% test_name )
  }
  tictoc::toc()
  
  
  
  # TEST: change the underlying data base to simulate data race error.
  sto2 <- get_sto(n = 10, seed = sample.int(10000000, 1))
  test_name <-
    "Change underlying table in database,  sto[, ]  != sqltable "
  tictoc::tic(test_name)
  
  # Update underlying table.
  tmp_c <- dbConnect(RSQLite::SQLite(), sto2$get_host)
  tmp <- dbSendQuery(tmp_c, glue("UPDATE {sto2$get_tablename} SET a = 1 WHERE a < 0"))
  dbClearResult(tmp)
  dbDisconnect(tmp_c)

  # Should generate an error.
  tryCatch({
    sto2[1,1] <- 0
    tests_assert$push(red("FAIL: ") %+% test_name )
  }, error = function(e){
    tests_assert$push(green("PASS: ") %+% test_name )
  })
  

  #
  # Create an empty table on the db.
  # Create a sto on that empty table.
  # Try putting only strings into the local df.
  # Write back should fail.
  
  
  # Disconnect (kills all temporary databases)
  ######DBI::dbDisconnect(con_temp)
    
}

# Print Asserts messages
tests_assert$getMessages() %+% "\n" %>% cat

