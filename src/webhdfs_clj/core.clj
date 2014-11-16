(ns webhdfs-clj.core
  (:require
    [webhdfs-clj.util :as u]
    [webhdfs-clj.auth :as a]
    [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.tools.logging :as log]
            [clj-http.lite.client :as http])
  (:import [java.io StringWriter]))

(defn- throw-exception [json-data]
  (let [{:keys [javaClassName message]} (:RemoteException json-data)]
    (throw
      (clojure.lang.Reflector/invokeConstructor
        (resolve (symbol javaClassName))
        (to-array [message])))))

(defn- remote-exception? [response]
  (contains? response :RemoteException))

(defn- request [method path opts]
  (println opts)
  (let [failure? (fn [status] (not (and (>= status 200) (< status 400))))
        query-opts
        (into {} (for [[k v] (:query-params opts) :when (not (nil? v))]
                   [(name k) (if (keyword? v) (name v) (str v))])) 
        as (:as opts)
        {:keys [status body]}
        (http/request
          (merge opts
                 {
                  :headers (:headers opts)
                  :method method
                  :query-params query-opts
                  :throw-exceptions false
                  :url (str (u/base-url) path)}))]
    ;(log/info status body)
    (if (failure? status)
      (throw-exception body)
      (if (= as :json)
        (json/read-str body :key-fn keyword)
        body))))

(defn- http-get [path query-opts & {:keys [as] :or {as :json}}]
  (request :get path {:as as :query-params query-opts}))

(defn- http-put [path query-opts & {:keys [as] :or {as :json}}]
  (request :put path {:as as :query-params query-opts}))

(defn- http-post [path query-opts & {:keys [as] :or {as :json}}]
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
