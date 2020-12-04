import deserialize.TransactionDeserializer
import model.Transaction
import model.UserResidenceDatabase
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import reader.IncomingTransactionsReader
import java.util.*
import java.util.concurrent.ExecutionException

const val SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions"
const val VALID_TRANSACTIONS_TOPIC = "valid-transactions"
const val BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"

fun main() {
    val kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS)
    processTransactions(IncomingTransactionsReader, UserResidenceDatabase, kafkaProducer)
    try {
        processTransactions(IncomingTransactionsReader, UserResidenceDatabase, kafkaProducer)
    } catch (e : ExecutionException) {
        e.printStackTrace()
    } catch (e: InterruptedException) {
        e.printStackTrace()
    } finally {
        kafkaProducer.flush()
        kafkaProducer.close()
    }
}

@Throws(ExecutionException::class, InterruptedException::class)
private fun processTransactions(incomingTransactionReader: IncomingTransactionsReader,
                                userResidenceDatabase: UserResidenceDatabase,
                                kafkaProducer: Producer<String, Transaction>): Unit {
    while (incomingTransactionReader.hasNext()) {
        val transaction = incomingTransactionReader.next()

        /**
         * Fill in you code here.
         * Send the transaction to the right topic based on the origin of the transaction and the user's residence data
         */
    }
}

private fun createKafkaProducer(bootstrapServers: String): Producer<String, Transaction> {
    val props = Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "banking-api-service")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, String::class.java.name)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TransactionDeserializer::class.java.name)
    return KafkaProducer(props)
}