package lu.magalhaes.gilles.provxlib
package utils

class NotificationsConfig(path: String) {
  private val config = SafeConfiguration.fromLocalPath(path).get

  def token: String = config.getString("notifications.token").get

  def user: String = config.getString("notifications.user").get

}
