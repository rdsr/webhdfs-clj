package webhdfs_clj;

import java.io.*;
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
    Authenticator.setDefault(new MyAuthenticator());
    final URL url = new URL("http://eat1-nertznn01.grid.linkedin.com:50070/webhdfs/v1/tmp/dh1?op=CREATE");
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty("Expect", "100-continue");
    //conn.setInstanceFollowRedirects(false);
    conn.setDoOutput(true);
    conn.setRequestMethod("PUT");
    conn.setDoInput(true);

    conn.connect();
    //System.err.print(conn.getResponseCode());

    OutputStream outputStream = conn.getOutputStream();
    FileInputStream f = new FileInputStream("/home/rratti/tmp/diff.patch");
    byte[] b = new byte[4028];
    while (f.read(b) != -1) {
      System.err.println("...");
      outputStream.write(b);
    }
    outputStream.close();
    String temp = null;
    BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    StringBuilder sb = new StringBuilder();
    while((temp = in.readLine()) != null){
      sb.append(temp).append(" ");
    }
    System.err.println(conn.getResponseCode());
  }
}
