

tmp <- list(a=1)
class(tmp) <- "myclass"
'[<-.myclass' <-
  function(o,
           x,
           y,
           value) {
    Nargs <- nargs() - 1
    missingx <- missing(x)
    missingy <- missing(y)
    missingv <- missing(value)
    print(paste("Nargs", Nargs))
    print(paste("missingx", missingx))
    print(paste("missingy", missingy))
    print(paste("missingv", missingv))
    
    # tmp <- o$subset_write(x = x, y = y, value = value, Nargs = nargs() - 1, missingx = missing(x), 
    #                       missingy = missing(y), missingvalue = missing(value))
    # return(tmp)
    return("done")
  }
tmp[1] <- 1




'[<-.myclass' <- function(o, x, y, value) {
  #print(names(sys.call()))
  Nargs <- nargs() - 1
  missingx <- !missing(x)
  missingy <- !missing(y)
  missingv <- !missing(value)
  
  print(paste("Nargs", Nargs))
  print(paste("missingx", missingx))
  print(paste("missingy", missingy))
  print(paste("missingv", missingv))
  
  # if(Nargs == 1 && !gx && !gy){
  #   print("m[]")
  # }
  # if(Nargs == 1 && gx && !gy){
  #   print("m[1]")
  # }
  # if(Nargs == 2 && gx && !gy){
  #   print("m[1, ]")
  # }
  # if(Nargs == 2 && !gx && gy){
  #   print("m[, 1]")
  # }
  # if(Nargs == 2 && gx && gy){
  #   print("m[1, 1]")
  # }
  # if(Nargs == 2 && !gx && !gy){
  #   print("m[, ]")
  # }
  
  invisible("")
}


#//////////////////////////////////////
# Assignment with subsett
source("./src/objects.R")
#source("./tests/smalltableobject_test.R")

# Create data and an object.
tf <- tempfile()
create_test_data(conn = DBI::dbConnect(drv = RSQLite::SQLite(), dbname = tf))
sto2 <- NULL
sto2 <- SmallTableObject$new(dbtype = "sqlite", host = db, tablename = "test1")


# Test: Pin one unique row. Update 1 cell. Compare to data base.  
tmprow <- sample(nrow(sto2[]), 1)
tmpdata <- sto2[tmprow, ]
sto2[tmprow, 1] <- (tmpdata[, 1] - 1)

conn <- DBI::dbConnect(drv = RSQLite::SQLite(), dbname = tf)
res <- DBI::dbGetQuery(conn, 
glue("SELECT a FROM 
test1 
WHERE b = {tmpdata$b}
AND c = '{tmpdata$c}'
                "))
DBI::dbDisconnect(conn)

res
sto2[tmprow, 1]
(tmpdata[, 1] - 1)

sto2[] <- 1 #nargs:1 missing:TTF
sto2[1] <- c(1,1) #nargs:1 missing:FTF
sto2[1 ] <- c(1,1)#nargs:1 missing:FTF
sto2[1, ] <- 1 #nargs:2 missing:FTF
sto2[, 1] <- 1 #nargs:2 missing:TFF
sto2[1, 1] <- 1 #nargs:2 missing:FFF

#//////////////////////////////////////
# Subsetting
source("./src/objects.R")
sto2 <- NULL
sto2 <- SmallTableObject$new(dbtype = "sqlite", host = db, tablename = "test1")
sto2[]
sto2[,"z"]
cars[, "z"]
sto2[1]
sto2[1, ]
sto2[, 1]
sto2[1, 1]

sto2[2, ]
sto2[1]
sto2[, 1]
sto2[NULL]
cars[NULL]
sto2[NA]
cars[NA]


####
f1 <- function(z){
  asscol <- checkmate::makeAssertCollection() 
  checkmate::assert_true(F, add = asscol)
  checkmate::assert_true(F, add = asscol)
  checkmate::assert_true(F, add = asscol)
  checkmate::assert_true(z, add = asscol)
  #asscol$getMessages()
}
f1(z = T)
