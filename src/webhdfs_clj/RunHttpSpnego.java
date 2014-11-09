package webhdfs_clj;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.LoginContext;


public class RunHttpSpnego {
  static final String kuser = "***REMOVED***"; // your account name
  static final String kpass = "***REMOVED***"; // your password for the account

  static class MyAuthenticator extends Authenticator {
    @Override
    public PasswordAuthentication getPasswordAuthentication() {
      // I haven't checked getRequestingScheme() here, since for NTLM
      // and Negotiate, the usrname and password are all the same.
      System.err.println("Feeding username and password for " + getRequestingScheme());
      return (new PasswordAuthentication(kuser, kpass.toCharArray()));
    }
  }

  public static void extractToken(HttpURLConnection conn) throws Exception {
    if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
      final Map<String, List<String>> headers = conn.getHeaderFields();
      final List<String> cookies = headers.get("Set-Cookie");
      if (cookies != null) {
        for (final String cookie : cookies) {
          final String AUTH_COOKIE_EQ = "hadoop.auth=";
          if (cookie.startsWith(AUTH_COOKIE_EQ)) {
            String value = cookie.substring(AUTH_COOKIE_EQ.length());
            final int separator = value.indexOf(";");
            if (separator > -1) {
              value = value.substring(0, separator);
            }
            if (value.length() > 0) {
              System.err.println(value);
            }
          }
        }
      }
    } else {
      throw new Exception("Authentication failed, status: " + conn.getResponseCode() + ", message: "
          + conn.getResponseMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    //LoginContext lc = new LoginContext("com.sun.security.jgss.krb5.initiate");
    //  lc.login();
    Authenticator.setDefault(new MyAuthenticator());
    final URL url = new URL("http://eat1-euchrenn01.grid.linkedin.com:50070/webhdfs/v1/tmp?op=GETFILESTATUS");
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("OPTIONS");
    conn.connect();
    if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
      extractToken(conn);
    }
    final InputStream ins = conn.getInputStream();
    final BufferedReader reader = new BufferedReader(new InputStreamReader(ins));
    String str;
    while ((str = reader.readLine()) != null) {
      System.out.println(str);
    }
  }
}
