(ns kotoba.p2p.graphsync-replication
  "Fail-closed checkpoint replication plans and verified availability receipts."
  (:require [ipld.value :as value]))

(def snapshot-version 1)

(defn- invalid! [message data]
  (throw (ex-info message (assoc data :type :graphsync/invalid-replication))))

(defn- positive! [config key]
  (let [n (get config key)]
    (when-not (and (integer? n) (pos? n))
      (invalid! "replication requires a positive limit" {:limit key :value n}))
    n))

(defn new-tracker [config]
  (doseq [key [:replication-factor :max-replicas :receipt-ttl-ms]]
    (positive! config key))
  (when (> (:replication-factor config) (:max-replicas config))
    (invalid! "replication factor exceeds replica limit" {:config config}))
  {:config (select-keys config [:replication-factor :max-replicas :receipt-ttl-ms])
   :plans {} :receipts {}})

(defn- authorize! [authorize-fn context]
  (when-not (and (fn? authorize-fn) (true? (authorize-fn context)))
    (throw (ex-info "replication plan is unauthorized"
                    {:type :graphsync/unauthorized
                     :principal (:principal context)}))))

(defn plan-replication
  "Choose distinct replicas deterministically and return host transfer effects."
  [tracker cid replica-ids principal authorize-fn]
  (authorize! authorize-fn {:action :checkpoint/replicate
                            :cid cid :principal principal})
  (let [replicas (->> replica-ids distinct (sort-by pr-str)
                      (take (get-in tracker [:config :max-replicas])) vec)
        tracker (assoc-in tracker [:plans cid] (set replicas))]
    {:tracker tracker
     :effects (mapv (fn [replica-id]
                      {:effect :checkpoint/replicate :cid cid :replica-id replica-id})
                    replicas)
     :sufficient-candidates?
     (<= (get-in tracker [:config :replication-factor]) (count replicas))}))

(defn record-receipt
  "Record one signature-verified receipt from a planned replica. A replica id
  can contribute at most one live receipt per CID."
  [tracker {:keys [cid replica-id stored-at-ms] :as receipt}
   now-ms verify-receipt-fn]
  (when-not (contains? (get-in tracker [:plans cid] #{}) replica-id)
    (invalid! "receipt is from an unplanned replica" {:cid cid :replica-id replica-id}))
  (when-not (and (integer? now-ms) (<= 0 now-ms)
                 (integer? stored-at-ms) (<= 0 stored-at-ms now-ms))
    (invalid! "receipt time is invalid"
              {:stored-at-ms stored-at-ms :now-ms now-ms}))
  (when-not (and (fn? verify-receipt-fn) (true? (verify-receipt-fn receipt)))
    (throw (ex-info "replication receipt verification failed"
                    {:type :graphsync/invalid-receipt
                     :cid cid :replica-id replica-id})))
  (assoc-in tracker [:receipts cid replica-id]
            (assoc receipt :recorded-at-ms now-ms
                   :expires-at-ms (+ now-ms (get-in tracker [:config :receipt-ttl-ms])))))

(defn qualification
  "Return evidence, never an optimistic HA claim. Only distinct, unexpired,
  signature-verified replica receipts count."
  [tracker cid now-ms]
  (let [live (->> (get-in tracker [:receipts cid] {})
                  (filter (fn [[_ receipt]] (< now-ms (:expires-at-ms receipt))))
                  (into {}))
        required (get-in tracker [:config :replication-factor])]
    {:cid cid :required required :actual (count live)
     :replicas (set (keys live)) :qualified? (<= required (count live))}))

(defn expire-receipts [tracker now-ms]
  (let [before (reduce + 0 (map count (vals (:receipts tracker))))
        receipts (into {}
                       (keep (fn [[cid by-replica]]
                               (let [live (into {} (filter (fn [[_ receipt]]
                                                             (< now-ms (:expires-at-ms receipt)))
                                                           by-replica))]
                                 (when (seq live) [cid live]))))
                       (:receipts tracker))
        after (reduce + 0 (map count (vals receipts)))]
    {:tracker (assoc tracker :receipts receipts) :expired (- before after)}))

(defn snapshot [tracker]
  (value/encode-value {:version snapshot-version :tracker tracker}))

(defn restore [bytes]
  (let [envelope (value/decode-value bytes)]
    (when-not (and (= snapshot-version (:version envelope))
                   (map? (:tracker envelope)))
      (invalid! "invalid replication snapshot" {}))
    (:tracker envelope)))
