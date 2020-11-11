// Returns if an integer is even

def is_even(num: Int) =(num%2==0)

is_even(4)
is_even(5)

def check_evens_list(nums:List[Int]): Boolean={
  for(num <- nums){
    if(num%2 == 0){
      return true
    }
  }
  return false
}

check_evens_list(List(1,3, 5))
check_evens_list(List(1,3,5,6))

def lucky_number_seven(nums: List[Int]): Int={
  var output=0
  for(num <- nums){
    if(num==7){
      output = output + 14
    }else{
      output = output + num
    }
  }
  return output
}

lucky_number_seven(List(1,6,7))
lucky_number_seven(List(7,7,7,10))

// Can you Balance?
def balanceCheck(mylist:List[Int]): Boolean ={
  var firsthalf = 0
  var secondhalf = 0

  secondhalf = mylist.sum

  for(i <- Range(0,mylist.length)){
    firsthalf = firsthalf + mylist(i)
    secondhalf = secondhalf - mylist(i)

    if(firsthalf == secondhalf){
      return true
    }
  }
  return false
}
// Check to see it worked
val ballist = List(1,2,3,4,10)
val ballist2 = List(2,3,3,2)
val unballist2 = List(10,20,70)

println(balanceCheck(ballist))
println(balanceCheck(ballist2))
println(balanceCheck(unballist2))

// Palindrome Check
def palindromeCheck(st: String): Boolean = {
  return (st == st.reverse)
}
println(palindromeCheck("abccba"))
println(palindromeCheck("abc"))
