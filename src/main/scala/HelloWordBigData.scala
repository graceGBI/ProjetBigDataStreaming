object HelloWordBigData {

  /** premier progamme Scala* */
  /** Les types de base en scala
   * Byte
   * Int
   * String
   * Boolean
   * Double, Char,...
   * Unit : type qui ne retourne aucune valeur (équivalent de void() en Java)
   * Any : un peu comme un fourre-tout Scala * */


  val ma_var_imm: String = "Juvenal"
  private val une_var_imm: String = "Formation Big Data"

  def main(args: Array[String]): Unit = {
    println("Hello world : mon premier programme en scala");

    var test_mu: Int = 15
    test_mu = test_mu + 10

    println(test_mu)

    /* variable est un pointeur vers une adresse memoire de la RAM
    * Il n'y a pas un intérêt à réserver un espace mémoire et à le bloquer
    * avec une valeur c'est plutot intéressant dans les pb de concurence pas dans la programmation classique*/

    val test_imm: Int = 15
    //Error : test_imm = 10+3

    println("Votre texte contient : " + Comptage_carac(" Hello la famille, wafasso ? ") + " caractères")

    getResult(10)

    testWhile(10)

    testFor()

  }

  //Ma first fonction
  def Comptage_carac(texte: String): Int = {
    texte.trim.length()
  }
  //syntaxe 2
  def Comptage_carac2(texte: String): Int = {
    return texte.trim.length()
  }
  //syntaxe 3
  def Comptage_carac3(texte: String): Int = texte.trim.length()

  //ma première méthode/procédure
  def getResult( parametre : Any) : Unit = {
    if(parametre == 10){
      println("Votre valeur est un entier")
    }else{
      println("Votre valeur n'est pas un entier")
    }
  }

  //structures conditionnelles
  def testWhile(valeur_cond : Int) : Unit ={
    var i : Int =0
    while(i<10){
      println("Intération N°: " + i)
      i=i+1
    }
  }

  def testFor() : Unit={
    var i : Int =0
    for(i <- 0 to 10 by 2){
      println("Intération For N°: " + i)
    }
  }
}

