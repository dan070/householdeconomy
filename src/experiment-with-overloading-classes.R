# Experiment with overloading the class based [ operator

sto2 <- SmallTableObject$new(dbtype = "sqlite", host = db, tablename = "test1")



'[.SmallTableObject' <- function(o, x, y){
  print("start"); 
  print("o:"); 
  print(o); 
  print("x"); 
  print(x); 
  print("y:"); 
  print(y); 
  print("fin")
  return("Return value")
  } 
rm(`[.SmallTableObject`)

'[<-.SmallTableObject' <- function(o, x, y, value){
  print("start"); 
  print("o:"); 
  print(o); 
  print("x"); 
  print(x); 
  print("y:"); 
  print(y); 
  print("fin")
  return(o)
} 
rm(`[<-.SmallTableObject`)



'.subset<-.SmallTableObject' <- function(o, x, y, value){
  print("start"); 
  print("o:"); 
  print(o); 
  print("x"); 
  print(x); 
  print("y:"); 
  print(y); 
  print("fin")
  return("Return value")
} 


rm(`.subset<-.SmallTableObject`)

sto2[1]

sto2[1, 1] <- 100
sto2[, 1] <- 100

class(sto2)
class