(ns webhdfs-clj.auth
  (:require
    [webhdfs-clj.util :as u]
    [clojure.tools.logging :as log])
  (:import
    [java.net CookieHandler CookieManager URL HttpURLConnection
              Authenticator PasswordAuthentication]
    (java.util Date Calendar)))

(defn- credentials? []
  (if (and
        (contains? (u/cfg) :user)
        (contains? (u/cfg) :password))
    true
    (do (log/info "No user credentials provided in configuration")
        false)))

(defn set-auth-mechanism! []
  (CookieHandler/setDefault (CookieManager.))
  (when (credentials?)
    (Authenticator/setDefault
      (proxy [Authenticator] []
        (getPasswordAuthentication []
          (log/info "Registering password authentication for user: " (u/cfg :user))
          (PasswordAuthentication.
            (u/cfg :user)
            (into-array Character/TYPE (u/cfg :password))))))))




