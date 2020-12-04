package model

import java.util.*

/**
 * Mock database that contains a map from a user to its country of residence
 */
object UserResidenceDatabase {

    private const val USER_RESIDENCE_FILE = "user-residence.txt"
    private val userToResidenceMap: Map<String, String>

    init {
        userToResidenceMap = loadUserResidenceFromFile()
    }

    /**
     * Returns the user's country of residence
     */
    fun getUserResidence(user: String): String {
        if (!userToResidenceMap.containsKey(user)) {
            throw RuntimeException("user $user doesn't exist")
        }
        return userToResidenceMap.get(user)!!
    }

    private fun loadUserResidenceFromFile(): Map<String, String> {
        val userToResidence = mutableMapOf<String, String>()
        val inputStream = this.javaClass.classLoader.getResourceAsStream(USER_RESIDENCE_FILE)
        val scanner = Scanner(inputStream)
        while (scanner.hasNextLine()) {
            val line = scanner.nextLine()
            val userResidencePair = line.split(" ")
            userToResidence[userResidencePair[0]] = userResidencePair[1]
        }
        return userToResidence
    }
}