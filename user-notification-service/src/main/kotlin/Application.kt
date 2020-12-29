import deserialize.TransactionDeserializer
import model.Transaction
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

const val SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions"
const val BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"

fun main() {
    val consumerGroup = "user-notification-service"
    println("Consumer is part of consumer group $consumerGroup")
    val kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup)
    consumeMessages(SUSPICIOUS_TRANSACTIONS_TOPIC, kafkaConsumer)
}

private fun consumeMessages(topic: String, kafkaConsumer: Consumer<String, Transaction>) {
    kafkaConsumer.subscribe(mutableListOf(topic))
    while (true) {
        val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1))
        if (consumerRecords.isEmpty) {
            continue
        }
        consumerRecords.forEach {
            println("Received record (key: ${it.key()}, value: ${it.value()}, " +
                    "partition: ${it.partition()}, offset: ${it.offset()}\n\n")
            sendUserNotification(it.value())
        }
    }
}

private fun sendUserNotification(transaction: Transaction) {
    println("Sending user ${transaction.user} notification about " +
            "a suspicious transaction of ${transaction.amount} in their account " +
            "originating in ${transaction.transactionLocation}")
}

private fun createKafkaConsumer(bootstrapServers: String, consumerGroup: String): Consumer<String, Transaction> {
    val props = Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TransactionDeserializer::class.java.name)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)
    return KafkaConsumer(props)
}