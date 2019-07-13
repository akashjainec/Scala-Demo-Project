

object LoopsPractice {
  
  def main(args: Array[String]){

  
  val names = Seq("chris", "ed", "maurice")
  val nums = Seq(1, 2, 3)
  val letters = Seq('a', 'b', 'c')

/*for (n <- names) println(n)
for (n <- names) println(n.capitalize)    
for (n <- names) {
    // imagine this requires several lines
    println(n.capitalize)
}*/
  
 /* val res = for {
    n <- nums
    c <- letters
} yield (n, c)
println(res)*/
  for (i <- 0 until names.length) {
    println(s"$i is ${names(i)}")
}
  
  for ((name, count) <- names.zipWithIndex) {
    println(s"$count is $name")
}
  }
}