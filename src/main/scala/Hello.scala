/**
 * Created by alvaro on 8/25/15.
 */

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.{HttpHosts, Constants}
import com.twitter.hbc.core.endpoint.{StatusesFirehoseEndpoint, StatusesSampleEndpoint, StatusesFilterEndpoint}
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import scala.collection.JavaConverters._

object Hello extends App {

  def stream(consumerKey: String, consumerSecret: String, token:String, secret: String): Unit = {

    val queue = new LinkedBlockingQueue[String](10000)
    val hosts = new HttpHosts(Constants.STREAM_HOST)

    val filter = new StatusesFilterEndpoint()
    val terms = List[String]("basketball", "baseball", "football")
    filter.trackTerms(terms.asJava)
    filter.stallWarnings(false)


    val auth = new OAuth1(consumerKey, consumerSecret, token, secret)

    val client = new ClientBuilder()
                        .name("SampleClient")
                        .hosts(hosts)
                        .endpoint(filter)   // filter | sample | firehose
                        .authentication(auth)
                        .processor(new StringDelimitedProcessor(queue))
                        .build()

    client.connect()

    while(! client.isDone) {
      val msg = queue.take()
      println(msg)
    }
  }


  val consumerKey: String = scala.util.Properties.envOrElse("CONSUMER_KEY", "consumerKey" )
  val consumeSecret: String = scala.util.Properties.envOrElse("CONSUMER_SECRET", "consumerSecret" )
  val token: String = scala.util.Properties.envOrElse("TOKEN", "token" )
  val secret: String = scala.util.Properties.envOrElse("SECRET", "secret" )

  stream(consumerKey, consumeSecret, token, secret)

}

