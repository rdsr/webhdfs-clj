(defproject webhdfs-clj "0.1.0"
  :jvm-opts ["-Dsun.security.krb5.debug=true"]
  :description "Clojure client library for WebHDFS Rest API"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/data.json "0.2.5"]
                 [org.clojure/tools.logging "0.3.1"]
                 [clj-http "1.0.0" :exclusions [cheshire crouton org.clojure/tools.reader]]
                 [org.apache.hadoop/hadoop-auth "2.3.0" :exclusions [[org.apache.httpcomponents/httpclient]]]])
