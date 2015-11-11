import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord



object KafkaProducerGenerator {

  def getNewProducer(brokerList:String): KafkaProducer[String, String] = {
    val kafkaProps = new Properties
    kafkaProps.put("bootstrap.servers", brokerList)
   // kafkaProps.put("metadata.broker.list", brokerList)

    // This is mandatory, even though we don't send keys
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("acks", "0")

    // how many times to retry when produce request fails?
    kafkaProps.put("retries", "3")
    kafkaProps.put("linger.ms", "2")
    kafkaProps.put("batch.size", "1000")
    //kafkaProps.put("queue.time", "2")

    new KafkaProducer[String, String](kafkaProps)
  }


  def main(args:Array[String]):Unit = {

  val brokerlist="neutron:9092"
  val topic = "gamer"
  val testmessage = "This is a test of the KafkaProducer"
  val producer = getNewProducer(brokerlist)

  val message = new ProducerRecord[String, String](topic, testmessage)
  producer.send(message)
    producer.close()


}


}