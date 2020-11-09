import org.mockito.MockitoSugar.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IgniteServiceTest extends AnyFlatSpec with Matchers {

  behavior of "IgniteService"

  val hadoopService = mock[Hadoop]

  val igniteService = new IgniteService()

  it should "produce empty results if no data provided" in {

    val result = igniteService.runAggregation()
    val expectedResult = "AGE,AVG(US.SALARY),AVG(UT.TRIP)"

    //Then
    result should be (expectedResult)
  }

  it should "correctly aggregate data" in {
    //Given
    val input  = Seq(
      "1,2,3",
      "2,5,10",
      "1,19,3",
      "2,29,5"
    )

    //When
    input.foreach(in => igniteService.add(in))
    val result = igniteService.runAggregation()
    val expectedResult = "AGE,AVG(US.SALARY),AVG(UT.TRIP)\n18-25,3.0,3\n25-35,10.0,5"

    //Then
    result should be (expectedResult)
  }

}
