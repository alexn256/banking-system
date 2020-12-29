package deserialize

import com.fasterxml.jackson.databind.ObjectMapper
import model.Transaction
import org.apache.kafka.common.serialization.Deserializer

/**
 * Kafka Deserializer implementation.
 * Deserializes a Transaction from JSON to a [Transaction] object
 */
object TransactionDeserializer : Deserializer<Transaction> {
    override fun deserialize(topic: String?, data: ByteArray?): Transaction {
        val mapper = ObjectMapper()
        var transaction:Transaction? = null
        try {
            transaction = mapper.readValue(data, Transaction::class.java)
        } catch (e: Exception) {
            e.printStackTrace()
        }
        return transaction!!
    }
}