(defproject webhdfs-clj "0.1.0"
            :jvm-opts [;"-Dsun.security.krb5.debug=true"
                       "-Djava.security.auth.login.config=conf/headless.conf"]
            :resource-paths ["conf"]
            :description "Clojure client library for WebHDFS Rest API"
            :dependencies [[org.clojars.rdsr/clj-http-lite "0.2.1-SNAPSHOT"]
                           [org.clojure/data.json "0.2.5"]
                           [org.clojure/tools.logging "0.3.1"]
                           [org.clojure/clojure "1.6.0"]])
