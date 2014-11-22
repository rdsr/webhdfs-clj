(ns webhdfs-clj.core
  (:refer-clojure :exclude [concat])
  (:require
    [webhdfs-clj.util :as u]
    [webhdfs-clj.auth :as a]
    [clojure.java.io :as io]
    [clojure.string :as s]
    [clojure.data.json :as json]
    [clojure.tools.logging :as log]
    [clj-http.lite.client :as http]
    [clojure.java.io :as io])
  (:import [java.io IOException]
           [clojure.lang Reflector]
           (java.net URI URL)))

;; setup authentication
(a/setup-auth!)

(defn- resolve-class [class-name]
  (try
    (resolve (symbol class-name))
    (catch ClassNotFoundException _
      (log/warn "Could not resolve class" class-name ". Using IOException instead")
      IOException)))

(defn- throw-exception [json-data]
  (let [{:keys [javaClassName message]} (:RemoteException json-data)]
    (throw
      (Reflector/invokeConstructor
        (resolve-class javaClassName)
        (to-array [message])))))

(defn- abs-url [uri]
  (let [uri (URI. uri)]
    (if (.isAbsolute uri)
      (.toURL uri)
      (URL. (str (u/base-url) (.getPath uri))))))

(defn- request [method uri opts]
  (let [url (abs-url uri)
        failure? (complement http/unexceptional-status?)
        query-opts
        (into {} (for [[k v] (:query-params opts) :when (not (nil? v))]
                   [(name k) (if (keyword? v) (name v) (str v))]))]
    (log/info "Executing request, method:" method ", uri:" uri ", query:" query-opts)
    (let [{:keys [status body headers]}
          (http/request
            (merge opts
                   {:headers          (:headers opts)
                    :method           method
                    :follow-redirects false
                    :throw-exceptions false
                    :query-params     query-opts
                    :url              url}))]
      (log/info "Received status: " status)
      (cond
        (failure? status) (throw-exception (json/read-str body :key-fn keyword))
        ;; Webhdfs REST API only gives TEMPORARY_REDIRECT. In this case
        ;; pass url back so that a proper request can be made
        (= status 307) (headers "location")
        (= (:as opts) :json) (json/read-str body :key-fn keyword)
        :else body))))


(defn- http-get [uri query-opts & {:as opts}]
  (request :get uri (merge {:as :json} opts {:query-params query-opts})))

(defn- http-put [uri query-opts & {:as opts}]
  (request :put uri (merge {:as :json} opts {:query-params query-opts})))

(defn- http-post [uri query-opts & {:as opts}]
  (request :post uri (merge {:as :json} opts {:query-params query-opts})))

(defn- http-delete [uri query-opts & {:keys [as] :or {as :json} :as opts}]
  (request :delete uri (merge {:as :json} opts {:query-params  query-opts})))

(defn- parse-url [url]
  (let [{:keys [scheme server-name server-port uri query-string]} (http/parse-url url)]
    [(str (name scheme) "://" server-name
          (when server-port (str ":" server-port))
          uri)
     (when query-string
       (into {} (map (fn [e] (s/split e #"=")) (s/split query-string #"&"))))]))

(defn create
  [uri entity & {:keys [encoding overwrite block-size replication permission buffer-size]}]
  (let [url (http-put uri
                      {:op          :create
                       :overwrite   overwrite
                       :blocksize   block-size
                       :replication replication
                       :permission  permission
                       :buffersize  buffer-size})
        [url query-params] (parse-url url)]
    (http-put url
              query-params
              :body entity
              :body-encoding encoding
              :as :string)
    'ok))

(defn append [uri entity & {:keys [encoding buffer-size]}]
  (let [url (http-post uri {:op :append :buffersize buffer-size})
        [url query-params] (parse-url url)]
    (http-post url query-params :body entity :body-encoding encoding :as :string))
  'ok)

(defn concat [uri sources]
  (http-post uri {:op :concat :sources (s/join "," sources)})
  'ok)

(defn mkdir [uri & {:keys [permission]}]
  (let [r (http-put uri {:op :mkdirs :permission permission})]
    (:boolean r)))

(defn create-symlink [uri destination & {:keys [createParent] :or {createParent false}}]
  (let [r (http-put uri {:op :createsymlink :destination destination :createParent createParent} :as :byte-array)]
    (:boolean r)))

(defn rename [uri destination]
  (let [r (http-put uri {:op :rename :destination destination})]
    (:boolean r)))

(defn delete [uri & {:keys [recursive] :or {recursive false}}]
  (let [r (http-delete uri {:op :delete :recursive recursive})]
    (:boolean r)))

(defn get-file-status [uri]
  (let [r (http-get uri {:op :getfilestatus})]
    (get r :FileStatus)))

(defn list-status [uri]
  (let [r (http-get uri {:op :liststatus})]
    (get-in r [:FileStatuses :FileStatus])))

(defn get-content-summary [uri]
  (http-get uri {:op :getcontentsummary}))

(defn get-file-checksum [uri]
  (let [r (http-get uri {:op :getfilechecksum})]
    (:FileChecksum r)))

(defn get-home-directory []
  (let [r (http-get "/" {:op :gethomedirectory})]
    (:uri r)))

(defn set-permission [uri permission]
  (http-put uri {:op :setpermission :permission permission} :as :byte-array)
  'ok)

(defn set-owner [uri & {:keys [owner group]}]
  ;; owner and group are optional in webhdfs rest-api spec
  (http-put uri {:op :setowner :owner owner :group group}))

(defn set-replication [uri & {:keys [replication]}]
  ;; replication is optional in webhdfs rest-api spec
  (let [r (http-put uri {:op :setreplication :replication replication})]
    (:boolean r)))

(defn set-times
  [uri & {:keys [modification-time access-time]}]
  (http-put uri {:op :settimes :modificationtime modification-time :accesstime access-time} :as :byte-array)
  'ok)

(defn get-delegation-token [renewer]
  (let [r (http-get "/" {:op :getdelegationtoken :renewer renewer})]
    (get-in r [:Token :urlString])))

(defn get-delegation-tokens [renewer]
  (let [r (http-get "/" {:op :getdelegationtokens :renewer renewer})]
    (map :urlString (get-in r [:Tokens :token]))))

(defn renew-delegation-token [token]
  (let [r (http-put "/" {:op :renewdelegationtoken :token token})]
    (:long r)))

(defn cancel-delegation-token [token]
  (http-put "/" {:op :canceldelegationtoken :token token})
  'ok)
