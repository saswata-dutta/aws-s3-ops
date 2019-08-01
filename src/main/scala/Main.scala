import java.time.{Instant, ZoneId, ZonedDateTime}

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}

import scala.io.Source

object Main {

  def main(args: Array[String]): Unit = {
    val bucket = "???"
    val s3client: AmazonS3 = AmazonS3Client.builder().withRegion(Regions.AP_SOUTH_1).build()

    val IST: ZoneId = ZoneId.of("Asia/Kolkata")

    val commonPrefix = "transaction_details/y=2019/m=08/d=01"
    val oldPrefix = s"$commonPrefix/p=XXXX"

    val infile = "~/keys.txt"
    for (name <- Source.fromFile(infile).getLines) {
      val oldKey = s"$oldPrefix/$name"
      val ms = name.reverse.slice(18, 31).reverse.toLong

      val i = Instant.ofEpochMilli(ms)
      val zdt = ZonedDateTime.ofInstant(i, IST)
      val h = zdt.getHour
      val hour = f"h=$h%02d"
      val newKey = s"$commonPrefix/$hour/p=XXXX/$name"

      s3client.copyObject(bucket, oldKey, bucket, newKey)
      s3client.deleteObject(bucket, oldKey)
      println(s"$oldKey -> $newKey")
    }
  }
}
