(ns kotoba.p2p.graphsync-replication-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing] :include-macros true])
            [kotoba.p2p.graphsync-replication :as replication]))

(def config {:replication-factor 2 :max-replicas 3 :receipt-ttl-ms 100})
(def allow (constantly true))
(def verify (constantly true))

(deftest availability-requires-distinct-live-verified-receipts
  (let [cid "bafy-checkpoint"
        planned (replication/plan-replication
                 (replication/new-tracker config) cid ["b" "a" "a"]
                 "peer:owner" allow)
        tracker (:tracker planned)
        one (replication/record-receipt
             tracker {:cid cid :replica-id "a" :stored-at-ms 10 :signature [1]}
             10 verify)
        two (replication/record-receipt
             one {:cid cid :replica-id "b" :stored-at-ms 11 :signature [2]}
             11 verify)]
    (is (:sufficient-candidates? planned))
    (is (= ["a" "b"] (mapv :replica-id (:effects planned))))
    (is (false? (:qualified? (replication/qualification one cid 11))))
    (is (:qualified? (replication/qualification two cid 11)))
    (is (false? (:qualified? (replication/qualification two cid 111))))
    (is (= two (-> two replication/snapshot replication/restore)))
    (let [{:keys [tracker expired]} (replication/expire-receipts two 111)]
      (is (= 2 expired))
      (is (zero? (:actual (replication/qualification tracker cid 111)))))))

(deftest authorization-signature-and-placement-fail-closed
  (let [cid "bafy-checkpoint"
        tracker (replication/new-tracker config)]
    (is (thrown? #?(:clj Exception :cljs js/Error)
                 (replication/plan-replication tracker cid ["a" "b"] "peer:x"
                                               (constantly false))))
    (let [tracker (:tracker (replication/plan-replication
                             tracker cid ["a"] "peer:x" allow))]
      (testing "an unplanned replica and an invalid signature never land"
        (is (thrown? #?(:clj Exception :cljs js/Error)
                     (replication/record-receipt
                      tracker {:cid cid :replica-id "b" :stored-at-ms 0} 0 verify)))
        (is (thrown? #?(:clj Exception :cljs js/Error)
                     (replication/record-receipt
                      tracker {:cid cid :replica-id "a" :stored-at-ms 0} 0
                      (constantly false))))))))
