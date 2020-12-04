import model.Transaction
import org.apache.kafka.clients.consumer.Consumer

private const val VALID_TRANSACTIONS_TOPIC = "valid-transactions"
private const val SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions"
private const val BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"

fun main() {
    /**
     *         String consumerGroup = /** Decide on the name for the consumer group.**/

    System.out.println("Consumer is part of consumer group " + consumerGroup);

    Consumer<String, Transaction> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);

    consumeMessages(Collections.unmodifiableList(Arrays.asList(SUSPICIOUS_TRANSACTIONS_TOPIC, VALID_TRANSACTIONS_TOPIC)), kafkaConsumer);
     */
}

private fun createKafkaConsumer(bootstrapServers: String, consumerGroup: String): Consumer<String, Transaction> {
    /**
     * Fill in the code here to subscribe to the provided topics
     * Run in a loop and consume all the transactions
     * Record the transactions for reporting based on the topic
     */
}

private fun consumeMessages(topics: List<String>, kafkaConsumer: Consumer<String, Transaction>):Unit {
    /**
     * Configure all the Kafka client parameters here
     * Create and return new Kafka consumer
     */
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