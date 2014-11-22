(ns webhdfs-clj.auth
  (:require
    [webhdfs-clj.util :as u]
    [clojure.tools.logging :as log])
  (:import
    [java.net CookieHandler CookieManager Authenticator PasswordAuthentication]))

(defn- user-creds?
  "Returns true if user (principal) and passwords are
  provided in config file."
  []
  (if (and
        (contains? (u/cfg) :user)
        (contains? (u/cfg) :password))
    true
    (do (log/info "No user credentials provided in configuration")
        false)))

(defn setup-auth! []
  ;; setup default cookie manger for cookies
  ;; provided by the webhdfs rest service
  (CookieHandler/setDefault (CookieManager.))
  (when (user-creds?)
    (Authenticator/setDefault
      (proxy [Authenticator] []
        (getPasswordAuthentication []
          (log/info "Registering password authentication for user: " (u/cfg :user))
          (PasswordAuthentication.
            (u/cfg :user)
            (into-array Character/TYPE (u/cfg :password))))))))

;(setup-auth!)
