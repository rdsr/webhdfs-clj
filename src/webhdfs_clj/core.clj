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

(def conf {:host "eat1-euchrenn01.grid.linkedin.com" :port 50070 :http-debug true})

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
  (if (or (nil? @cookie)
          (expired? @cookie))
    (let [url (str (base-url) "/?op=GETFILESTATUS")]
      (swap! cookie (fn [_] (str "hadoop.auth=" (authenticate url)))))
    @cookie))

(defn- throw-exception [json-data]
  (let [{:keys [javaClassName message]} (:RemoteException json-data)]
    (throw
      (clojure.lang.Reflector/invokeConstructor
        (resolve (symbol javaClassName))
        (to-array [message])))))

(defn- remote-exception? [response]
  (contains? response :RemoteException))

(defn- request [method path opts]
  (let [failure? (fn [status] (not (and (>= status 200) (< status 400))))
        query-opts
        (into {} (for [[k v] (:query-params opts) :when (not (nil? v))]
                   [(name k) (if (keyword? v) (name v) (str v))])) 
        as (:as opts)
        {:keys [status body]}
        (http/request
          (merge opts
                 {:debug (:http-debug conf)
                  :headers (assoc (:headers opts) :cookie (get-cookie!))
                  :method method
                  :query-params query-opts
                  :throw-exceptions false
                  :url (str (base-url) path)}))]
    ;(log/info status body)
    (if (failure? status)
      (throw-exception body)
      (if (= as :json)
        (json/read-str body :key-fn keyword)
        body))))

(defn- http-get [path query-opts & {:keys [as] :or {or :json}}]
  (request :get path {:as as :query-params query-opts}))

(defn- http-put [path query-opts & {:keys [as] :or {or :json}}]
  (request :put path {:as as :query-params query-opts}))

(defn- http-post [path query-opts & {:keys [as] :or {or :json}}]
  (request :post path {:as as :query-params query-opts}))

(defn create 
  [path [& {:keys [overwrite block-size replication permission buffer-size]}]]
  (request :put path 
    {:op :create :overwrite overwrite :blocksize block-size 
     :replication replication :permission permission :buffersize buffer-size}))     

(defn append [path [& {:keys [buffer-size]}]]
  (request :post path {:op :append :buffersize buffer-size}))

(defn xconcat [path sources]
  (request :post path {:op concat :sources (clojure.string/join "," sources)}))

(defn mkdir [path & {:keys [permission]}]
  (let [r (http-put path {:op :mkdirs :permission permission})]
    (:boolean r)))

(defn create-symlink [path destination & {:keys [createParent] :or {createParent false}}]
  (let [r (http-put path {:op :createsymlink :createParent createParent})]
    (:boolean r)))

(defn rename [path destination]
  (let [r (http-put path {:op :rename})]
    (:boolean r)))

(defn delete [path & {:keys [recursive] :or {recursive false}}]
  (let [r (http-put path {:op :delete :recursive recursive})]
    (:boolean r)))

(defn get-file-status [path]
 (let [r (http-get path {:op :getfilestatus})]
  (get-in [:FileStatuses :FileStatus]))) 

(defn list-status [path]
  (http-get path {:op :liststatus}))

(defn get-content-summary [path]
  (http-get path {:op :getcontentsummary}))

(defn get-file-checksum [path]
  (http-get path {:op :getfilechecksum}))

(defn get-home-directory []
  (http-get "/" {:op :gethomedirectory}))

(defn set-permission [path permission]
  (http-put path {:op :setpermission :permission permission}))

(defn set-owner [& {:keys [owner group]}]
  (http-put "/" {:op :setowner :owner owner :group group}))

(defn set-replication [& {:keys [replication]}]
  (let [r (http-put "/" {:op :setreplication :replication replication})]
    (:boolean r)))

(defn set-times
  [& {:keys [modification-time access-time]}] 
  (http-put {:op :settimes :modificationtime modification-time :accesstime access-time})
  'ok)

(defn get-delegation-token [renewer]
  (let [r (http-get {:op :getdelegationtoken :renewer renewer})]
    (:urlString r)))

(defn get-delegation-tokens [renewer]
  (let [r (http-get {:op :getdelegationtokens :renewer renewer})]
    (map :urlString (get-in r [:Tokens :token]))))

(defn renew-delegation-token [token]
  (let [r (http-put {:op :renewdelegationtoken :token token})]
    (:long r)))

(defn cancel-delegation-token [token]
  (http-put {:op :canceldelegationtoken :token token})
  'ok)
