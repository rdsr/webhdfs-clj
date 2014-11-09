(defproject webhdfs-clj "0.1.0"
  :jvm-opts ["-Dsun.security.krb5.debug=true -Djava.security.auth.login.config=login.conf -Djavax.security.auth.useSubjectCredsOnly=false"]
  :resource-paths ["conf"]
  :description "Clojure client library for WebHDFS Rest API"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/data.json "0.2.5"]
                 [org.clojure/tools.logging "0.3.1"]
                 [http-kit "2.1.18"]
                 [clj-http "1.0.0" :exclusions
                  [cheshire crouton org.clojure/tools.reader]
                 ]])
