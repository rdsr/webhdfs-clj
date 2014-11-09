(ns webhdfs-clj.auth
  (:require 
    [webhdfs-clj.util :as u]
    [clojure.tools.logging :as log])
  (:import 
    [java.net URL HttpURLConnection 
     Authenticator PasswordAuthentication]))

(defn credentials? []
  (if (and 
        (contains? (u/cfg) :user)
        (contains? (u/cfg) :password))
    true
    (do (log/info "No user credentials provided in configuration")
      false)))

(when (credentials?)
  (Authenticator/setDefault 
    (proxy [Authenticator] []
     (getPasswordAuthentication []
       (PasswordAuthentication. (u/cfg :user) (u/cfg :password))))))

(defn- extract-token [connection]
  (some 
    #(when (.startsWith % "hadoop.auth=") 
       %)
    (.. connection getHeaderFields (get "Set-Cookie"))))

(defn- authenticate [url]
  (let [url (if (instance? URL url) url (URL. url))
        connection (.openConnection url)]
    (.setRequestMethod "OPTIONS")
    (let [response-code (.getResponseCode connection)]
      (if (= response-code
             HttpURLConnection/HTTP_OK)
        (extract-token connection)
        (throw
          (Exception.
            (str "Error when authenticating to url:" url 
              ". Response code: " response-code)))))))

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

(defn- get-cookie! []
  (if (or (nil? @cookie) (expired? @cookie))
    (let [url (str (u/base-url) "/?op=GETFILESTATUS")]
      (swap! cookie (fn [_] (authenticate url))))
    @cookie))
