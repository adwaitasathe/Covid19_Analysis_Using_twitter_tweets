import twitter4j.conf.ConfigurationBuilder

case class Account(consumerKey: String, consumerSecret: String, accessToken: String, accessTokenSecret: String) {
  def getConfiguration: ConfigurationBuilder = {
    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
  }
}
