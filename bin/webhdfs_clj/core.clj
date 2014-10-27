(ns webhdfs-clj.core
  (:require [clojure.data.json :as json])
  (:import [java.net URL] 
           [org.apache.hadoop.security UserGroupInformation] 
           [org.apache.hadoop.security.authentication.client AuthenticatedURL])) 

;(try 
;  (.openConnection (AuthenticatedURL.) 
;    (java.net.URL. (str "http://eat1-euchrenn01.grid.linkedin.com:50070/webhdfs/v1/" 
;                   "data/tracking/EndorsementsSuggestionImpressionEvent/daily/2014/10/06/part-00000.avro?op=OPEN"))
;    t)
;  (catch Exception e
;    (.printStackTrace e)
;    (throw e)))
;
;(defn http-put [path 

(def conf {:hostname "rratti-ld1"
         :user "rratti"
         :port 50070})

(defn url [path]
  (let [{:keys [scheme hostname port]} conf] 
    (str scheme hostname ":" port path)))

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
  