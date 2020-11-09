import java.util.logging.Logger

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.{IgniteAtomicSequence, Ignition}

class IgniteService(hadoopService: HadoopService) {

  val igniteConfig = new IgniteConfiguration().setIgniteInstanceName("SalariesAndTrips")
  val ignite = Ignition.start(igniteConfig)
  val user = ignite.createCache[String, User]("user")
  initializeCaches()

  val seqSalary: IgniteAtomicSequence = ignite.atomicSequence("salary", 0, true)
  val seqTrip: IgniteAtomicSequence = ignite.atomicSequence("trip", 0, true)

  def add(row: String): Unit = {
    val lines = row.split("\n").filter(_.nonEmpty)
    lines.foreach { line =>
      val splitData = line.split(",")
      if (splitData(1).toInt <= 12)
        addUserSalary(splitData)
      else addUserTrip(splitData)
    }
  }

  private def addUserSalary(data: Array[String]): Unit = {
    val id = seqSalary.incrementAndGet()
    val passport = data(0)
    val month = data(1)
    val salary = data(2)
    user.query(new SqlFieldsQuery(s"insert into user_salary (id, passport, month, salary) values ($id, '$passport', $month, $salary)"))
  }

  private def addUserTrip(data: Array[String]): Unit = {
    def ageCategory(age: String): String = {
      age.toInt match {
        case a if a >= 18 && a < 25 => "18-25"
        case a if a >= 25 && a < 35 => "25-35"
        case _ => "35+"
      }
    }
    val id = seqTrip.incrementAndGet()
    val passport = data(0)
    val age = ageCategory(data(1))
    val trip = data(2)
    user.query(new SqlFieldsQuery(s"insert into user_trip (id, passport, age, trip) values ($id, '$passport', '$age', $trip)"))
  }

  def initializeCaches(): Unit = {
    user.query(
      new SqlFieldsQuery("create table user_salary (id int not null primary key, passport varchar(50), month smallint, salary double precision)")
    )
    user.query(
      new SqlFieldsQuery("create table user_trip (id int not null primary key, passport varchar(50), age varchar(10), trip int)")
    )
  }

  def runAggregation(): Unit = {
    println(s"Started aggregation")
    val cache = ignite.cache("user")
    val userSalary =cache.query(new SqlFieldsQuery(
      "select * from user_salary"
    ))
    val userTrip = cache.query(new SqlFieldsQuery(
      "select * from user_trip"
    ))

    userSalary.forEach(println)
    userTrip.forEach(println)


    val res = cache.query(new SqlFieldsQuery(
      "select ut.age, avg(us.salary), avg(ut.trip) from user_salary as us \n" +
      "LEFT JOIN user_trip as ut ON us.passport = ut.passport GROUP BY ut.age"
    ))
    val header = (0 until res.getColumnsCount).map(indx => res.getFieldName(indx)).mkString(",")
    var values = ""
     res.forEach{c =>
      values += "\n" + (0 until res.getColumnsCount).map(idx => c.get(idx)).mkString(",")
    }
    hadoopService.save(s"$header$values")
    println(s"Finished aggregation")
  }
}
