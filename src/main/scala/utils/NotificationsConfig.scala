package lu.magalhaes.gilles.provxlib
package utils

class NotificationsConfig(path: String) {
  private val config = ConfigurationUtils.load(path)

  def token: Option[String] =
    ConfigurationUtils.getString(config.get, "notifications.token")

  def user: Option[String] =
    ConfigurationUtils.getString(config.get, "notifications.user")

}
