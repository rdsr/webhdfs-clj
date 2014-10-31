(ns webhdfs-clj.core
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.tools.logging :as log]
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
      (swap! cookie (fn [_] (str "hadoop.auth=" (authenticate url))))) 
    @cookie))

(defn- cleanup-query-params
  [query-params]
  (into 
    {} (map (fn [[k v]] 
              [(name k)
               (if (keyword? v) (name v) (str v))])
            query-params))) 
            
(defn- http-put [path query-params]
  (let [{:keys [status headers body]}
         (http/put (str (base-url) path)
                   {:debug true
                    :throw-exceptions false
                    :query-params (cleanup-query-params query-params)
                    :headers {:cookie (get-cookie!)}
                    })]
    (log/info "Received status code: " status)  
    [status headers body]))

;;(defn- http-post [path query-params]
  
(defn- throw-exception [json-data]
  (let [{:keys [javaClassName message]} (:RemoteException json-data)]
    (throw 
      (clojure.lang.Reflector/invokeConstructor 
        (resolve (symbol javaClassName)) 
        (to-array [message])))))

(defn- remote-exception? [response]
  (contains? response :RemoteException))

(defn mkdir [path & {:keys [permissions] :or {permissions ""}}]
  (let [response (http-put path {:op :MKDIRS :permissions permissions})]
    (if (remote-exception? response)
      (throw-exception response)
      (:boolean response))))

(defn create-symlink [path destination & {:keys [createParent] :or {createParent false}}]
  (let [response (http-put path {:op :CREATESYMLINK :createParent permissions})]
    (if (remote-exception? response)
      (throw-exception response)
      true)))

(defn rename [path destination]
  (let [response (http-put path {:op :RENAME})]
    (if (remote-exception? response)
      (throw-exception response)
      (:boolean response))))

(defn delete [path & {:keys [recursive] :or {recursive false}}]
  (let [response (http-put path {:op :DELETE :recursive recursive})]
    (if (remote-exception? response)
      (throw-exception response)
      (:boolean response))))