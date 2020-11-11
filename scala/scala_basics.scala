//////////////////
// CONTROL FLOW //
//////////////////
if(true){
  println("I will print if True.")
}

if(3==3){
  println("3 is equal to 3")
}

val x = "hello"

if(x.endsWith("o")){
  println("The value of x ends with o")
}else{
  println("The value of x does not end with o")
}

val person = "George"

if(person == "Sammy"){
  println("Welcome Sammy")
}else if(person== "George"){
  println("Welcome George")
}else{
  println("What is your name?")
}

///////////////////////
// LOGICAL OPERATORS //
///////////////////////

// AND - &&
println((1==2)&&(2==2))
println((1==1)&&(2==2))

// OR - ||
println((1==2) || (2==2))

// NOT - !()
println((1==1))
println(!(1==1))

///////////////////////
////// FOR LOOPS //////
///////////////////////

// Iterate through List
for(item <- List(1,2,3)){
  println(item)
}

// Iterate through an array
for(num <- Array.range(0,5)){
  println(num)
}

// Iterate through a set
for(num <- Set(1,2,3)){
  println(num)
}

for(num <- Range(1,11)){
  if(num%2 == 0){
    println(s"$num is even")
  }else{
    println(s"$num is odd")
  }
}

val names = List("John","Abe","Cindy","Cat")

for(name <-names){
  if(name.startsWith("C")){
    println(s"$name starts with a C")
  }
}

///////////////////////
///// WHILE LOOPS /////
///////////////////////

var x = 0

while(x < 5){
  println(s"x is currently $x")
  println("x is still less than 5, adding 1 to x")
  x= x+1
}

// Import break keyword
import util.control.Breaks._
var y = 0

while (y < 10){
  println(s"y is currently $y")
  println("y is still less than 10, add 1 to y")
  y = y+1
  if(y == 5) break
}

///////////////////////
////// FUNCTIONS //////
///////////////////////

def simple_function(): Unit = {
  println("simple print")
}

simple_function()

def adder(num1: Int, num2: Int): Int={
  return num1 + num2
}

adder(4,5)

def greetName(name:String): String={
  return s"Hello $name"
}

val fullgreet = greetName("Nick")
println(fullgreet)

def isPrime(numcheck:Int): Boolean={
  for(n <- Range(2,numcheck)){
    if(numcheck%n == 0){
      return false
    }
  }
  return true
}

println(isPrime(10))  // false
println(isPrime(23))  // true

val numbers = List(1,2,3,7)

def check(nums:List[Int]): List[Int]={
  return nums
}

println(check(numbers))