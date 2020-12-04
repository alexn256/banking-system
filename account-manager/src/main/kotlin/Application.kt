import deserialize.TransactionDeserializer
import model.Transaction
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

const val VALID_TRANSACTIONS_TOPIC = "valid-transactions"
const val BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"

fun main() {
    val consumerGroup = "account-manager"
    println("Consumer is part of consumer group $consumerGroup")
    val kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup)
    consumeMessages(VALID_TRANSACTIONS_TOPIC, kafkaConsumer)
}

fun consumeMessages(topic:String, kafkaConsumer:Consumer<String, Transaction>):Unit {
    kafkaConsumer.subscribe(listOf(topic))
    while (true) {
        val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1))
        if (consumerRecords.isEmpty) {
            //do something else.
        }
        for (record in consumerRecords) {
            println("Received record (key: ${record.key()}, value: ${record.value()}, partition: ${record.partition()}, offset: ${record.offset()}")
        }
        /**
         * Fill in the code here
         * Check if there are new transaction to read from Kafka.
         * Approve the incoming transactions
         */
        kafkaConsumer.commitAsync()
    }
}

fun createKafkaConsumer(bootstrapServers:String, consumerGroup:String):Consumer<String, Transaction> {
    val props = Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TransactionDeserializer::class.java.name)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
    return KafkaConsumer(props)
}

fun approveTransaction(transaction: Transaction) {
    println("Authorizing transaction for user ${transaction.user}, in the amount of $${transaction.amount}")
}