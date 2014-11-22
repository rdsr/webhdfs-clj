(ns webhdfs-clj.core
  (:refer-clojure :exclude [concat])
  (:require [clj-http.lite.client :as http]
            [clojure.data.json :as json]
            [clojure.string :as s]
            [clojure.tools.logging :as log]
            [webhdfs-clj.auth :as a]
            [webhdfs-clj.util :as u])
  (:import (clojure.lang Reflector)
           (java.net URI URL)
           (java.io IOException)))

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

(defn- query-params-as-strings
  [opts]
  (into {} (for [[k v] opts :when (not (nil? v))]
             [(name k) (if (keyword? v) (name v) (str v))])))

(defn- request [method uri opts]
  (let [url (abs-url uri)
        failure? (complement http/unexceptional-status?)
        query-opts (-> opts :query-params query-params-as-strings)]
    (log/info "Executing request, method:" method ", uri:" uri ", query:" query-opts)
    (let [{:keys [status body headers]}
          (http/request
            (merge {:method           method
                    :follow-redirects false
                    :throw-exceptions false
                    :url              url}
                   (assoc opts :query-params query-opts)))]
      (log/info "Received status: " status)
      (cond
        (failure? status)
        (throw-exception (json/read-str body :key-fn keyword))
        ;; Webhdfs REST API only returns TEMPORARY_REDIRECT in
        ;; cases of PUT and APPEND. In these cases, we pass
        ;; url back so that a separate request can we made
        ;; to the right datanode (specified in the redirected url)
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
  (request :delete uri (merge {:as :json} opts {:query-params query-opts})))

(defn- parse-url [url]
  (let [{:keys [scheme server-name server-port uri query-string]} (http/parse-url url)]
    [(str (name scheme) "://" server-name
          (when server-port (str ":" server-port))
          uri)
     (when query-string
       (into {} (map (fn [e] (s/split e #"=")) (s/split query-string #"&"))))]))

;; Webhdfs REST API methods
;; See http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html

(defn open [uri {:keys [offset length buffersize]}]
  (http-get uri
            {:op :open :offset offset :length length :buffersize buffersize}
            :as :stream
            :follow-redirects true))

(defn create
  [uri entity & {:keys [encoding overwrite block-size replication permission buffer-size]}]
  ;; I can't get HTTURlConnection to properly do redirect
  ;; with PUT body, hence doing it manually here.
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
