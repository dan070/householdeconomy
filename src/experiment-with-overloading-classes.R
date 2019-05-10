# Experiment with overloading the class based [ operator
source("./src/objects.R")

sto2 <- NULL
sto2 <- SmallTableObject$new(dbtype = "sqlite", host = db, tablename = "test1")
class(sto2)

rm(`[.SmallTableObject`)
'[.SmallTableObject' <- function(o = NULL, x = NULL, y = NULL){
  tmp <- o$subset(x = x, y = y, value = NULL)
  return(tmp)
  } 
sto2[, ] # Should work.
sto2[1, 1] # Should work.
sto2[, 1] # Should work.
sto2[-1, ] # Should work.
sto2[, -1] # Should work.
sto2[1010101010101, ] # Should kind of work, return NA for all values.
sto2[, 1010101010101] # Should not work.


rm(`[<-.SmallTableObject`)
'[<-.SmallTableObject' <- function(o = NULL, x = NULL, y = NULL, value = NULL){
  tmp <- o$subset(x = x, y = y, value = value)
  return(tmp)
} 
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

