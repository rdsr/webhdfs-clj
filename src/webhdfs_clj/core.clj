(ns webhdfs-clj.core
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clj-http.client :as http])
  (:import [java.net URL HttpURLConnection]
           [java.io StringWriter]
           [org.apache.hadoop.security.authentication.client 
            AuthenticatedURL AuthenticatedURL$Token]))

(def conf {:host "eat1-euchrenn01.grid.linkedin.com" :port 50070})

(defn- authenticate [url]
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
               
(defn- expiration-time [cookie]
  (java.util.Date.
    (Long/valueOf 
      (second 
        (.split 
          (first 
            (filter #(.startsWith % "e=") 
                    (.split cookie "&"))) 
          "=")))))

(defn- expired? [cookie]
  (let [cal (java.util.Calendar/getInstance)]
    ;; expire a few seconds early
    (.add cal java.util.Calendar/SECOND 10)
    (<= (compare (expiration-time cookie)
                 (.getTime cal))
        0)))

(def ^:private cookie (atom nil))

(defn- base-url []
  (let [{:keys [host port]} conf]
    (str "http://" host ":" port "/webhdfs/v1")))

(defn- get-cookie! []
  (if (or (nil? @cookie) (expired? @cookie))
    (let [url (str (base-url) "/tmp?op=GETFILESTATUS")]
      (swap! cookie (fn [_] (authenticate url))))
    (str "hadoop.auth=" @cookie)))

(get-cookie!)
  
(defn http-put [path query-params]
  (let [{:keys [status headers body error]}
         (http/put (str (base-url) path)
                   {:debug true
                    :query-params query-params
                    :headers {:cookie (get-cookie!)}})]
    [status headers body error]))

(defn- throw-exception [json-data]
  (let [{:keys [javaClassName message]} (:RemoteException json-data)]
    (throw 
      (clojure.lang.Reflector/invokeConstructor 
        (resolve (symbol javaClassName)) 
        (to-array [message])))))

(defn- remote-exception? [response]
  (contains? response :RemoteException))

(http-put "/tmp/abc" {:op "MKDIRS" :permissions "755"})

(defn mkdir [path & [permissions]]
  (let [response (http-put path {:op :MKDIRS :permissions permissions})]
    (if (remote-exception? response)
      (throw-exception response)
      (:boolean response))))

        