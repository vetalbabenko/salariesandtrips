import java.util.logging.Logger

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.{IgniteAtomicSequence, Ignition}

// Ignite service which manipulates with ignite caches and run aggregation on it.
class IgniteService() {

  val igniteConfig = new IgniteConfiguration().setIgniteInstanceName("SalariesAndTrips")
  val ignite = Ignition.start(igniteConfig)
  val user = ignite.createCache[String, User]("user")
  initializeCaches()

  val seqSalary: IgniteAtomicSequence = ignite.atomicSequence("salary", 0, true)
  val seqTrip: IgniteAtomicSequence = ignite.atomicSequence("trip", 0, true)

  // Adding row to cache. Based on row, if it salary or trip row then it should
  // added to corresponding cache.
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

  // Initializes caches
  private def initializeCaches(): Unit = {
    user.query(
      new SqlFieldsQuery("create table user_salary (id int not null primary key, passport varchar(50), month smallint, salary double precision)")
    )
    user.query(
      new SqlFieldsQuery("create table user_trip (id int not null primary key, passport varchar(50), age varchar(10), trip int)")
    )
  }

  // Aggregates user trips and saaries.
  def runAggregation(): String = {
    println(s"Started aggregation")
    val cache = ignite.cache("user")

    // Use left join to match row which corresponds to user salary and trips.
    val res = cache.query(new SqlFieldsQuery(
      "select ut.age, avg(us.salary), avg(ut.trip) from user_salary as us \n" +
      "LEFT JOIN user_trip as ut ON us.passport = ut.passport GROUP BY ut.age"
    ))
    val header = (0 until res.getColumnsCount).map(indx => res.getFieldName(indx)).mkString(",")
    var values = ""
     res.forEach{c =>
      values += "\n" + (0 until res.getColumnsCount).map(idx => c.get(idx)).mkString(",")
    }
    println(s"Finished aggregation")
    s"$header$values"
  }
}
