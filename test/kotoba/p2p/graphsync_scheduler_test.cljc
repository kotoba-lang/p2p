(ns kotoba.p2p.graphsync-scheduler-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing] :include-macros true])
            [ipld.core :as ipld]
            [kotoba.p2p.graphsync :as gs]
            [kotoba.p2p.graphsync-scheduler :as scheduler]))

(def config {:max-active 2 :max-blocks-per-message 1
             :max-block-bytes-per-message 4096})
(def traversal-limits {:max-blocks 8 :max-bytes 4096
                       :max-depth 8 :max-matches 16})
(def wire-limits {:max-message-bytes 65536 :max-requests 8
                  :max-responses 8 :max-blocks 8})

(defn request-id [seed]
  #?(:clj (byte-array (map unchecked-byte (range seed (+ seed 16))))
     :cljs (js/Uint8Array.from (clj->js (range seed (+ seed 16))))))

(def recursive-selector
  {:selector :explore-recursive
   :limit {:mode :none}
   :sequence {:selector :explore-union
              :members [{:selector :matcher}
                        {:selector :explore-all
                         :next {:selector :explore-recursive-edge}}]}})

(defn fixture []
  (let [store (atom {})
        put! (fn [cid bytes] (swap! store assoc cid bytes))
        leaf (ipld/put-node! put! {"name" "leaf"})
        middle (ipld/put-node! put! {"child" (ipld/link leaf)})
        root (ipld/put-node! put! {"child" (ipld/link middle)})]
    {:store store :root root :blocks [root middle leaf]}))

(defn new-request [id root priority]
  {:id id :type :new :root root :priority priority
   :selector recursive-selector})

(deftest admission-acknowledges-and-chunks-partial-then-full
  (let [{:keys [store root blocks]} (fixture)
        request (new-request (request-id 0) root 3)
        admitted (scheduler/handle-message
                  (scheduler/new-scheduler config) #(get @store %)
                  {:requests [request]} traversal-limits)
        drained (scheduler/drain (:scheduler admitted) 4)
        messages (:messages drained)]
    (is (= 10 (get-in admitted [:message :responses 0 :status])))
    (is (= [14 14 20] (mapv #(get-in % [:responses 0 :status]) messages)))
    (is (= blocks (mapv #(get-in % [:blocks 0 :cid]) messages)))
    (is (zero? (scheduler/active-count (:scheduler drained))))
    (testing "every emitted chunk remains a valid GraphSync wire message"
      (is (= [14 14 20]
             (mapv #(-> %
                        (gs/encode-message wire-limits)
                        (gs/decode-message wire-limits)
                        (get-in [:responses 0 :status]))
                   messages))))))

(deftest priority-order-update-and-cancel-are-deterministic
  (let [{:keys [store root]} (fixture)
        low (new-request (request-id 0) root 1)
        high (new-request (request-id 32) root 9)
        admitted (scheduler/handle-message
                  (scheduler/new-scheduler config) #(get @store %)
                  {:requests [low high]} traversal-limits)
        updated (scheduler/handle-message
                 (:scheduler admitted) #(get @store %)
                 {:requests [{:id (:id low) :type :update
                              :extensions {"window" {"blocks" 1}}}]}
                 traversal-limits)
        first-step (scheduler/step (:scheduler updated))
        cancelled (scheduler/handle-message
                   (:scheduler first-step) #(get @store %)
                   {:requests [{:id (:id high) :type :cancel}]}
                   traversal-limits)]
    (is (= (vec (:id high))
           (-> first-step :message :responses first :id vec)))
    (is (= {"window" {"blocks" 1}}
           (get-in updated [:scheduler :active (vec (:id low)) :extensions])))
    (is (= 35 (get-in cancelled [:message :responses 0 :status])))
    (is (= #{(vec (:id low))}
           (scheduler/active-request-ids (:scheduler cancelled))))))

(deftest capacity-duplicates-missing-content-and-unknown-control-fail-closed
  (let [{:keys [store root]} (fixture)
        one (new-request (request-id 0) root 1)
        two (new-request (request-id 32) root 1)
        three (new-request (request-id 64) root 1)
        admitted (scheduler/handle-message
                  (scheduler/new-scheduler config) #(get @store %)
                  {:requests [one two three one]} traversal-limits)]
    (is (= [10 10 31 30]
           (mapv :status (get-in admitted [:message :responses]))))
    (let [unknown (request-id 96)
          controlled (scheduler/handle-message
                      (:scheduler admitted) #(get @store %)
                      {:requests [{:id unknown :type :cancel}
                                  {:id unknown :type :update
                                   :extensions {"x" true}}]}
                      traversal-limits)]
      (is (nil? (:message controlled)))
      (is (= [:unknown-cancel :unknown-update]
             (mapv :reason (:events controlled)))))
    (let [missing (new-request (request-id 96) "bafyreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku" 1)
          failed (scheduler/handle-message
                  (scheduler/new-scheduler config) #(get @store %)
                  {:requests [missing]} traversal-limits)]
      (is (= 34 (get-in failed [:message :responses 0 :status])))
      (is (zero? (scheduler/active-count (:scheduler failed)))))))

(deftest configured-resource-bounds-are-enforced
  (let [{:keys [store root]} (fixture)
        request (new-request (request-id 0) root 1)]
    (is (thrown? #?(:clj Exception :cljs js/Error)
                 (scheduler/new-scheduler (assoc config :max-active 0))))
    (let [tiny (scheduler/new-scheduler
                (assoc config :max-block-bytes-per-message 1))
          result (scheduler/handle-message tiny #(get @store %)
                                           {:requests [request]} traversal-limits)]
      (is (= 32 (get-in result [:message :responses 0 :status])))
      (is (= :block-too-large (get-in result [:events 0 :reason]))))
    (let [admitted (scheduler/handle-message
                    (scheduler/new-scheduler config) #(get @store %)
                    {:requests [request]} traversal-limits)]
      (is (thrown? #?(:clj Exception :cljs js/Error)
                   (scheduler/drain (:scheduler admitted) 2))))))
