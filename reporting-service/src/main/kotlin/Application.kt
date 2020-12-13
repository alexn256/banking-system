import deserialize.TransactionDeserializer
import model.Transaction
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.*

private const val VALID_TRANSACTIONS_TOPIC = "valid-transactions"
private const val SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions"
private const val BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"

fun main() {
    val consumerGroup = "reporting-service"
    println("Consumer is part of consumer group $consumerGroup")
    val kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup)
    consumeMessages(listOf(SUSPICIOUS_TRANSACTIONS_TOPIC, VALID_TRANSACTIONS_TOPIC), kafkaConsumer)
}

private fun createKafkaConsumer(bootstrapServers: String, consumerGroup: String): Consumer<String, Transaction> {
    val props = Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TransactionDeserializer::class.java.name)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
    return KafkaConsumer(props)
}

private fun consumeMessages(topics: List<String>, kafkaConsumer: Consumer<String, Transaction>):Unit {
    kafkaConsumer.subscribe(topics)
    while (true) {
        val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1))
        if (consumerRecords.isEmpty) {
            continue
        }
        consumerRecords.forEach {
            println("Received record (key: ${it.key()}, value: ${it.value()}, " +
                    "partition: ${it.partition()}, offset: ${it.offset()} from topic ${it.topic()}")
            recordTransactionForReporting(it.topic(), it.value())
        }
        kafkaConsumer.commitAsync()
    }
}

private fun recordTransactionForReporting(topic: String, transaction: Transaction):Unit {
    if (topic == SUSPICIOUS_TRANSACTIONS_TOPIC) {
        println("Recording suspicious transaction for user ${transaction.user}, amount of " +
                "${transaction.amount} originating in ${transaction.transactionLocation} for further investigation")
    } else if (topic == VALID_TRANSACTIONS_TOPIC) {
        println("Recording transaction for user ${transaction.user}, amount${transaction.amount} to show it on user's " +
                "monthly statement")
    }
}