(ns webhdfs-clj.auth
  (:require
    [webhdfs-clj.util :as u]
    [clojure.tools.logging :as log])
  (:import
    [java.net CookieHandler CookieManager Authenticator PasswordAuthentication]))

(defn- user-creds?
  "Returns true if user (principal) and passwords are
  provided in config file."
  [] (if (and (contains? (u/cfg) :user) (contains? (u/cfg) :password))
       true
       (do (log/info "No user credentials provided in configuration")
           false)))

(defn- secure?
  [] ((u/cfg) :secure false))

(defn setup-auth!
  "Setups up default cookie manager for HttpURLConnection.
   Also registers an Authenticator if user credentials are
   provided. This Authenticator provides user credentials
   for Http SPNEGO Negotiate"
  []
  (when (secure?)
    (CookieHandler/setDefault (CookieManager.)))
  (when (user-creds?)
    (Authenticator/setDefault
      (proxy [Authenticator] []
        (getPasswordAuthentication []
          (log/info "Registering password authentication for user: " (u/cfg :user))
          (PasswordAuthentication.
            (u/cfg :user)
            (into-array Character/TYPE (u/cfg :password))))))))
