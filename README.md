# `webhdfs-clj`

A Clojure client library for Hadoop WebHDFS Rest API.
A very light weight library without any Hadoop or Http client dependencies. Also works with a secure (kerborized) Hadoop cluster.

## Setup

### Configuration
The library expects a configuration file with name `conf/config.clj`. The file contains a clojure map with all the needed properties. See [conf/example-config.clj](conf/example-config.clj).

### Talking to a non-secure cluster
Remove the `:secure?` key from `config.clj` (or set it to `false`) and add the key `:user` . The value of the `:user` key is passed as the query parameter `user.name` to authenticate to a non-secure cluster.

### Talking to a secure Hadoop cluster.
Set the key `:secure?` to `true` in the configuration file `config.clj`

#### Using user credentials
 * Create a file `user.conf` similar to [conf/example-user.conf](conf/example-user.conf) under `conf` folder. Set the jvm property `java.security.auth.login.config` to `conf/user.conf`. (Infact `example-user.conf` can be used as is). When using a repl with [lein](https://github.com/technomancy/leiningen) the jvm properties can be specified in [project.clj](project.clj) under the key `:jvm-opts`.
 * Update [conf/config.clj](conf/config.clj) configuration file with `:user` and `:password`.

#### Using a headless user
 * Create a file `headless.conf` similar to [conf/example-headless.conf](conf/example-headless.conf) under `conf` folder. You will need to update the keys `keyTab` (with the keytab location) and `principal` (headless@DOMAIN.COM). Set the jvm property `java.security.auth.login.config` to `headless/user.conf`.

#### Notes 
 * Kerberos can make use of AES-128 or 256 to encrypt it's keys. For Java, to support AES-256, an addition JCE policy file must be install. These files can be downloaded from http://www.oracle.com/technetwork/java/javase/downloads/jce-7-download-432124.html
 * Make sure you have the right `krb5.conf` at `/etc/krb5.conf` or you may set the jvm property `java.security.krb5.conf` to the location of your `krb5.conf` file.

## Usage
```bash
# start up lein from the root of the project
lein repl
```
```clojure
(use 'webhdfs-clj.core)
(require '[clojure.java.io :as io])

;; Copy a file '/tmp/A' on to the cluster
(create "/tmp/A" "/home/rratti/A")

;; or
(create "/tmp/A" (io/file "/home/rratti/A")) 

;; This works too
(with-open [in (io/input-stream "/home/rratti/A")]
  (create "/tmp/A" in))
  
;; The second arg. to 'create' can be either a string, byte-array, inputstream or file

;; Copy a file '/tmp/A' from the cluster
(with-open [in (open "/tmp/A")]
  (io/copy in (io/file "/home/rratti/A")))
  
;; append to a file '/tmp/A' on the cluster
(append "/tmp/A" "/home/rratti/B")

;; Similarly the second arg to append can be either a string, byte-array, inputstream or file

;; Almost all the other methods are self-explanatory.
```

### Exceptions
If ever an exceptional status is returned from the server. It is converted to a java Exception. See [here](http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Error_Responses) for the mapping of Http error codes to Java Exception classes.


## License

Released under the MIT License:
<http://www.opensource.org/licenses/mit-license.php>
