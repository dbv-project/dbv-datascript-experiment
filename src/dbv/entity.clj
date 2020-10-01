(ns dbv.entity
  (:require [next.jdbc :as jdbc]))

(comment
  (do
    (def data-source
      (jdbc/get-datasource {:dbtype "postgres"
                            :dbname "postgres"
                            :user "postgres"
                            :password "postgres"}))

    (def connection
      (doto (jdbc/get-connection data-source)
        (.setAutoCommit false)))

    (require '[dbv.db :as db])

    (def db
      (-> (db/db {:connectable connection
                  :table "eavrt"})
          (assoc :basis-tx 13194139535699)))

    (require '[datascript.impl.entity :as e])
    )

  (time
   (def sample-entity
     (e/touch (e/entity db
                        17592186045569))))

  (e/touch
   (get-in sample-entity
           [:design/audio-selection
            :audio-selection/source]))

  (e/touch
   (e/entity db
             [:audio/uuid #uuid "5f4e107c-78c6-48b1-b9fd-c3ff0e295cb3"]))

  (:db/valueType (e/entity db
                           :db/valueType))

  (e/touch (e/entity db
                     :db/valueType))

  (e/touch (e/entity db
                     0))

  )
