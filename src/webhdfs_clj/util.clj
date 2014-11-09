(ns webhdfs-clj.util
  (:use [clojure.java.io :as io])
  (:import [java.io PushbackReader]))

(defn read-cfg []
  (with-open
    [rdr (-> (io/resource "config.clj") io/reader PushbackReader.)]
    (binding [*read-eval* false]
      (read rdr))))

(def ^:private cfg-state (atom (read-cfg)))
(defn cfg 
  ([] @cfg-state)
  ([key] (get cfg key)))

;; mostly for development
(def set-cfg! (partial swap! cfg-state))
(defn reset-cfg! []
  (reset! cfg-state (read-cfg)))

(defn base-url []
  (let [{:keys [host port]} (u/cfg)]
    (str "http://" host ":" port "/webhdfs/v1")))
