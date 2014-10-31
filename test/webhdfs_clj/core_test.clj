(ns webhdfs-clj.core-test
  (:require [clojure.test :refer :all]
            [webhdfs-clj.core :refer :all]))

(expiration-time "\"u=rratti&p=***REMOVED***&t=kerberos&e=1414469737167&s=xcIyxNley7n0l73qwBo7pjXek0M=\""
)
(expired? "\"u=rratti&p=***REMOVED***&t=kerberos&e=1414469737167&s=xcIyxNley7n0l73qwBo7pjXek0M=\"")

;;(authenticate (URL. (str "http://:50070/webhdfs/v1/" 
  ;;                       "data/tracking/EndorsementsSuggestionImpressionEvent/daily/2014/10/06/part-00000.avro?op=OPEN")))
;;(http-put "/tmp/abc" {:op "MKDIRS" :permissions "755"})