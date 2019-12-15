package utils

object EncryptionUtil extends Serializable {
  /**
    * Transaction details are encrypted and stored in kafka.
    * Before processing, we use EncryptionUtil to decrypt the messages.
    * */

  def decrypt(message:String):String  = {
    val decryptMessage = "Some decrypt message"
    return decryptMessage
  }

}
