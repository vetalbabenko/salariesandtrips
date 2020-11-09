import org.apache.ignite.Ignition
import org.apache.ignite.cache.query.{ScanQuery, SqlFieldsQuery, SqlQuery}
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.lang.{IgniteBiPredicate, IgniteRunnable}

import scala.util.Random

object SalariesAndTrips extends App {


  val igniteConfig = new IgniteConfiguration().setIgniteInstanceName("SalariesAndTrips")

  val ignite  =  Ignition.start(igniteConfig)

  val cache = ignite.createCache[String, User]("salaries")

  val users = List(
      User("p1", 1, 20),
      User("p2", 1, 10),
      User("p2", 1, 30),
      User("p2", 2, 50),
      User("p2", 2, 50),
      User("p3", 1, 50),
      User("p3", 2, 10),
      User("p3", 3, 80),
      User("p4", 4, 20),
      User("p4", 5, 20),
      User("p4", 5, 60),
      User("p4", 1, 20)
  )

  cache.query(new SqlFieldsQuery("create table users (id int not null primary key, passport varchar(50) , month int, salary double precision)"))
  users.foreach{u =>
    cache.query(new SqlFieldsQuery(s"insert into users (id, passport, month, salary) values (${Random.nextInt(100000)}, '${u.passport}', ${u.month}, ${u.salary})"))
  }

  ignite.compute().run(new IgniteRunnable {
    override def run(): Unit = {
      val cache = ignite.cache("salaries")
      val res = cache.query(new SqlFieldsQuery("select passport, month, avg(salary) from users group by passport, month"))
      println(res.getColumnsCount)

    }
  })


}



case class User(passport: String, month: Int, salary: Double)
