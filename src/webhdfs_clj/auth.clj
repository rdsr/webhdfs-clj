(ns webhdfs-clj.auth
  (:require
    [webhdfs-clj.util :as u]
    [clojure.tools.logging :as log])
  (:import
    [java.net CookieHandler CookieManager URL HttpURLConnection
     Authenticator PasswordAuthentication]
    (java.util Date Calendar)))

(defn credentials? []
  (if (and
        (contains? (u/cfg) :user)
        (contains? (u/cfg) :password))
    true
    (do (log/info "No user credentials provided in configuration")
      false)))

(CookieHandler/setDefault (CookieManager.))
(when (credentials?)
  (Authenticator/setDefault
    (proxy [Authenticator] []
     (getPasswordAuthentication []
       (PasswordAuthentication.
         (u/cfg :user)
         (into-array Character/TYPE (u/cfg :password)))))))


(defn- extract-token [connection]
  (some
    #(when (.startsWith % "hadoop.auth=") %)
    (.. connection getHeaderFields (get "Set-Cookie"))))

(defn- authenticate [url]
  (let [url (if (instance? URL url) url (URL. url))
        connection (.openConnection url)]
    (.setRequestMethod connection "OPTIONS")
    (let [response-code (.getResponseCode connection)]
      (println response-code)
      (if (= response-code
             HttpURLConnection/HTTP_OK)
        (extract-token connection)
        (throw
          (Exception.
            (str "Error when authenticating to url:" url
              ". Response code: " response-code)))))))

(authenticate (str (u/base-url) "?op=GETFILESTATUS"))

(defn- expiration-time [cookie]
  (Date.
    (Long/valueOf
      (second
        (.split
          (first
            (filter #(.startsWith % "e=")
                    (.split cookie "&")))
          "=")))))

(defn- expired? [cookie]
  (let [cal (Calendar/getInstance)]
    ;; expire a few seconds early
    (.add cal Calendar/SECOND 10)
    (<= (compare (expiration-time cookie)
                 (.getTime cal))
        0)))

(def ^:private cookie (atom nil))

(defn- get-cookie! []
  (if (or (nil? @cookie) (expired? @cookie))
    (let [url (str (u/base-url) "/?op=GETFILESTATUS")]
      (swap! cookie (fn [_] (authenticate url))))
    @cookie))
