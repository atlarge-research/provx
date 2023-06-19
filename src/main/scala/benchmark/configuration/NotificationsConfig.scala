package lu.magalhaes.gilles.provxlib
package benchmark.configuration

class NotificationsConfig(path: String) {
  private val config = SafeConfiguration.fromLocalPath(path).get

  def token: String = config.getString("notifications.token").get

  def user: String = config.getString("notifications.user").get

}
