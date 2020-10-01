(ns dbv.db-util)

(defn entid
  [db ident]
  (if (integer? ident)
    ident
    (get-in db
            [:ids
             ident])))

(defn ident
  [db eid]
  (if (keyword? eid)
    eid
    (get-in db
            [:keys
             eid])))

(defn attribute
  [db attrid]
  (let [a (entid db
                 attrid)]
    {:id a
     :ident (ident db
                   attrid)
     :value-type (get-in db
                         [:attribute-types
                          a])
     :is-component (contains? (:attribute-components db)
                              a)
     :cardinality (get-in db
                          [:attribute-cardinality
                           a])
     :unique (get-in db
                     [:attribute-unique
                      a])
     }))

(defn value-type
  [db a]
  (get (:attribute-types db)
       (entid db
              a)))

(defn column-name
  [db a]
  (-> (value-type db
                  a)
      (name)))

(defn a-column
  [db a]
  (keyword (column-name db a)))
