(ns dbv.id)

(def ^:const tx0 13194139533312)

(defn t->tx
  [t]
  (bit-or tx0
          t))

(comment
  (t->tx 0)
  (= (t->tx 2387)
     13194139535699)
  )

(defn tx->t
  [tx]
  (bit-xor tx0
           tx)
  )

(comment
  (tx->t tx0)
  (tx->t 13194139535699)
  )

(defn part
  [eid]
  (bit-shift-right eid 42))

(comment
  (part tx0)
  (part 17592186045481)
  )

(def ^:const txmax
  (t->tx (dec tx0)))
