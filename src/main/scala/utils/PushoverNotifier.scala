package lu.magalhaes.gilles.provxlib
package utils


object PushoverNotifier {

  def notify(config: NotificationsConfig, title: String, message: String): Unit = {
    requests.post(
      "https://api.pushover.net/1/messages.json",
      data = Map(
        "token" -> config.token.get,
        "user" -> config.user.get,
        "title" -> title,
        "message" -> message
      ))
  }

}
