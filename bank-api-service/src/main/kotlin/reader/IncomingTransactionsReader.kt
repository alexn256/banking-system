package reader

import model.Transaction
import java.util.*
import java.util.Collections.emptyList
import java.util.Collections.unmodifiableList

/**
 * Mocks an HTTP server that receives purchase transactions in real time
 */
object IncomingTransactionsReader : Iterator<Transaction> {

    private const val INPUT_TRANSACTIONS_FILE = "user-transactions.txt"
    private val transactions: List<Transaction>
    private val transactionIterator: Iterator<Transaction>

    init {
        transactions = loadTransactions()
        transactionIterator = transactions.iterator()
    }

    private fun loadTransactions(): List<Transaction> {
        val inputStream = this.javaClass.classLoader.getResourceAsStream(INPUT_TRANSACTIONS_FILE)
        val scanner = Scanner(inputStream)
        val transactions:List<Transaction> = emptyList()
        while (scanner.hasNextLine()) {
            val transaction = scanner.nextLine().split(" ")
            val user = transaction[0]
            val transactionLocation = transaction[1]
            val amount = transaction[2].toDouble()
        }
        return transactions
    }

    override fun hasNext() = transactionIterator.hasNext()

    override fun next() = transactionIterator.next()
}