(ns dbv.id)

(defn t->tx
  [t]
  (bit-or 13194139533312
          t))

(comment
  (t->tx 0)
  (= (t->tx 2387)
     13194139535699)
  )

(defn tx->t
  [tx]
  (bit-xor 13194139533312
           tx)
  )

(comment
  (tx->t 13194139533312)
  (tx->t 13194139535699)
  )

(defn part
  [eid]
  (bit-shift-right eid 42))

(comment
  (part 13194139533312)
  (part 17592186045481)
  )
