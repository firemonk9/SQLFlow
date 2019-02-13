package com.metlife

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.{ArrayList, Collections, List, Random}
import java.util.concurrent.atomic.AtomicInteger

class RandomNameGenerator(seed:Int) {

//  def apply( seed: Long): RandomNameGenerator = {
//    new RandomNameGenerator(seed)
//  }

  private def loadNames(resource: String) = {
    val names = new java.util.ArrayList[String]
    // use classloader that loaded the jar with this class to ensure we can get the csvs
    try {
      val br = new BufferedReader(new InputStreamReader(classOf[Delme].getClassLoader.getResourceAsStream(resource), StandardCharsets.UTF_8))
      try
        br.lines.toArray.map(a=>a.toString().trim).filter((line: String) => line.length > 0).map(a=>names.add(a))
      catch {
        case e: IOException =>
          throw new IllegalStateException(e)
      } finally if (br != null) br.close()
    }
    Collections.unmodifiableList(names)
  }

  var firstNamesOriginal: java.util.List[String]=null
  var lastNamesOriginal: java.util.List[String]=null
  var companiesOriginal: java.util.List[String]=null
  var prefixOriginal: java.util.List[String]=null

  //shuffle(new Random)
  private var firstNames:java.util.List[String] = null
  private var lastNames:java.util.List[String] = null
  private var companies:java.util.List[String] = null

  // use AtomicInteger so multiple threads can use this without getting the same index
  final private val firstNameIndex = new AtomicInteger(0)
  final private val lastNameIndex = new AtomicInteger(0)
  final private val companyIndex = new AtomicInteger(0)
  final private val prefixIndex = new AtomicInteger(0)

//  def this(random: Random) {
//    RandomNameGenerator.loadNames("inbot-testfixtures/firstnames.csv")
//    RandomNameGenerator.loadNames("inbot-testfixtures/lastnames.csv")
//    RandomNameGenerator.loadNames("inbot-testfixtures/companies.csv")
//    RandomNameGenerator.loadNames("inbot-testfixtures/prefix.csv")
//    shuffle(random)
//  }

  def load(seed: Long) {
    firstNamesOriginal=loadNames("inbot-testfixtures/firstnames.csv")
    lastNamesOriginal=loadNames("inbot-testfixtures/lastnames.csv")
    companiesOriginal=loadNames("inbot-testfixtures/companies.csv")
    prefixOriginal=loadNames("inbot-testfixtures/prefix.csv")
    //this(RandomNameGenerator.loadNames("inbot-testfixtures/firstnames.csv"), RandomNameGenerator.loadNames("inbot-testfixtures/lastnames.csv"), RandomNameGenerator.loadNames("inbot-testfixtures/companies.csv"), RandomNameGenerator.loadNames("inbot-testfixtures/prefix.csv"))
    shuffle(new Random(seed))
  }

  def shuffle(random: Random): Unit = { // use synchronizedList to avoid concurrency issues triggering premature duplicates (this actually happened with concurrent test execution)
    firstNames = Collections.synchronizedList(new java.util.ArrayList[String](firstNamesOriginal))
    Collections.shuffle(firstNames, random)
    firstNameIndex.set(0)
    lastNames = Collections.synchronizedList(new java.util.ArrayList[String](lastNamesOriginal))
    Collections.shuffle(lastNames, random)
    lastNameIndex.set(0)
    companies = Collections.synchronizedList(new java.util.ArrayList[String](companiesOriginal))
    Collections.shuffle(companies, random)
    companyIndex.set(0)
  }

  def nextFirstName: String = {
    val counter = firstNameIndex.incrementAndGet
    val index = counter % firstNames.size
    firstNames.get(index)
  }

  def nextLastName: String = lastNames.get(lastNameIndex.incrementAndGet % lastNames.size)

  def nextCompanyName: String = companies.get(companyIndex.incrementAndGet % companies.size)

  def nextPrefix: String = prefixOriginal.get(prefixIndex.incrementAndGet % prefixOriginal.size)

  def nextFullName: String = nextPrefix + " " + nextFirstName + " " + nextLastName

  /**
    * Useful to generate random sets of person fields and some derived fields.
    *
    * @return array of first name, lastname, company name, domain name, email address
    */

  def nextPerson: Person = {

    import java.util

    return  Person (nextFirstName, nextLastName, nextCompanyName, nextPrefix)
  }

}


//return Array[String](person.getFirstName, person.getLastName, person.getCompany, person.getDomainName, person.getEmail)

