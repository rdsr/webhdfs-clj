(ns webhdfs-clj.core
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [org.httpkit.client :as http])
  (:import [java.net URL HttpURLConnection]
           [java.io StringWriter]
           [org.apache.hadoop.security.authentication.client 
            AuthenticatedURL AuthenticatedURL$Token])) 

(defn authenticate [url]
  (let [url (if (instance? URL url) url (URL. url)) 
        token (AuthenticatedURL$Token.)
        connection (.openConnection (AuthenticatedURL.) url token)
        response-code (.getResponseCode connection)]
    (if (= response-code
           HttpURLConnection/HTTP_OK)
      (str token)
      (throw 
        (Exception. 
          (str "Error when authenticating to url:" url ". Response code: " response-code)))))) 
               
;;(authenticate (URL. (str "http://eat1-euchrenn01.grid.linkedin.com:50070/webhdfs/v1/" 
  ;;                       "data/tracking/EndorsementsSuggestionImpressionEvent/daily/2014/10/06/part-00000.avro?op=OPEN")))

(defn- expiration-time [cookie]
  (java.util.Date.
    (Long/valueOf 
      (second 
        (.split 
          (first 
            (filter #(.startsWith % "e=") 
                    (.split cookie "&"))) 
          "=")))))

(expiration-time "\"u=rratti&p=***REMOVED***&t=kerberos&e=1414469737167&s=xcIyxNley7n0l73qwBo7pjXek0M=\""
)
  
(defn- expired? [cookie]
  (<= (compare (expiration-time cookie)
               (java.util.Date.))
      0))

(expired? "\"u=rratti&p=***REMOVED***&t=kerberos&e=1414469737167&s=xcIyxNley7n0l73qwBo7pjXek0M=\"")
  
(def ^:private cookie (atom nil))

(defn- get-cookie! [url]
  (if (or (nil? @cookie) (cookie-expired? @cookie))
    (swap! cookie (fn [_] (authenticate url)))
    @cookie))
  
(def conf {:host ""
           :port 50070})

(defn- url [path parameters]
  (let [{:keys [host port]} conf]
    (URL. (str "http://" host ":" port "/webhdfs/v1/" path "?" 
               (s/join "&" (map (fn [[k v]] (str k "=" v)) parameters))))))

(defn http-put [path parameters]
  ;(http/put
  )

(defn throw-exception [json-data]
  (let [{:keys [javaClassName message]} (:RemoteException json-data)]
    (throw 
      (clojure.lang.Reflector/invokeConstructor 
        (resolve (symbol javaClassName)) 
        (to-array [message])))))

(defn remote-exception? [response]
  (contains? response :RemoteException))

(defn mkdir [path & [permissions]]
  (let [response (http-put path {:op :MKDIRS :permissions permissions})]
    (if (remote-exception? response)
      (throw-exception response)
      (:boolean response))))

(defn get-file-status [path]
  (json/read-str (http/get (url path))))
        