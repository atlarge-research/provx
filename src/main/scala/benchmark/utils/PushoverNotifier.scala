package lu.magalhaes.gilles.provxlib
package benchmark.utils

import benchmark.configuration.NotificationsConfig

object PushoverNotifier {

  def notify(config: NotificationsConfig, title: String, message: String): Unit = {
    requests.post(
      "https://api.pushover.net/1/messages.json",
      data = Map(
        "token" -> config.token,
        "user" -> config.user,
        "title" -> title,
        "message" -> message
      ))
  }

}
