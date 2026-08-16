(ns kotoba.p2p.graphsync-scheduler
  "Pure bounded response scheduler for GraphSync v2 request lifecycles.

  Admission performs the existing bounded, CID-verifying IPLD traversal once,
  then retains its finite block result for priority-ordered, backpressured wire
  emission. This makes cancellation effective before unsent chunks cross the
  transport boundary, but is intentionally not a resumable traversal engine:
  traversal CPU and reads happen at admission time."
  (:require [kotoba.p2p.graphsync :as gs]))

(defn- invalid! [message data]
  (throw (ex-info message (assoc data :type :graphsync/invalid-scheduler))))

(defn- positive! [config key]
  (let [value (get config key)]
    (when-not (and (integer? value) (pos? value))
      (invalid! "graphsync scheduler: positive limit required"
                {:limit key :value value}))
    value))

(defn new-scheduler
  "Create an empty scheduler. Required positive limits are `:max-active`,
  `:max-blocks-per-message`, and `:max-block-bytes-per-message`."
  [config]
  (doseq [key [:max-active :max-blocks-per-message :max-block-bytes-per-message]]
    (positive! config key))
  {:config (select-keys config
                        [:max-active :max-blocks-per-message
                         :max-block-bytes-per-message])
   :active {}
   :next-order 0})

(defn active-count [scheduler]
  (count (:active scheduler)))

(defn active-request-ids
  "Content-valued UUID byte vectors for active requests."
  [scheduler]
  (set (keys (:active scheduler))))

(defn- request-key [request]
  (vec (:id request)))

(defn- response [request status-code]
  {:id (:id request) :status status-code})

(defn- result-message [responses]
  (when (seq responses) {:responses (vec responses)}))

(defn- byte-length [bytes]
  #?(:clj (if (bytes? bytes) (alength ^bytes bytes) (count bytes))
     :cljs (if (vector? bytes) (count bytes) (.-length bytes))))

(defn- oversized-block? [scheduler block]
  (> (byte-length (:bytes block))
     (get-in scheduler [:config :max-block-bytes-per-message])))

(defn- admit-new [scheduler get-fn request traversal-limits]
  (let [key (request-key request)]
    (cond
      (contains? (:active scheduler) key)
      {:scheduler scheduler
       :response (response request (:rejected gs/status))
       :event {:type :request/rejected :reason :duplicate :id key}}

      (>= (active-count scheduler) (get-in scheduler [:config :max-active]))
      {:scheduler scheduler
       :response (response request (:failed-busy gs/status))
       :event {:type :request/rejected :reason :capacity :id key}}

      :else
      (try
        (let [fulfilled (gs/fulfill-request get-fn request traversal-limits)
              blocks (vec (:blocks fulfilled))]
          (if-let [block (first (filter #(oversized-block? scheduler %) blocks))]
            {:scheduler scheduler
             :response (response request (:failed-unknown gs/status))
             :event {:type :request/rejected :reason :block-too-large
                     :id key :cid (:cid block)}}
            (let [entry {:request request
                         :priority (or (:priority request) 1)
                         :order (:next-order scheduler)
                         :extensions (or (:extensions request) {})
                         :blocks blocks
                         :offset 0}]
              {:scheduler (-> scheduler
                              (assoc-in [:active key] entry)
                              (update :next-order inc))
               :response (response request (:acknowledged gs/status))
               :event {:type :request/admitted :id key
                       :blocks (count blocks)}})))
        (catch #?(:clj clojure.lang.ExceptionInfo :cljs :default) error
          (let [error-type (:type (ex-data error))
                status-code (case error-type
                              :ipld/missing-block (:content-not-found gs/status)
                              :ipld/resource-limit (:failed-unknown gs/status)
                              (throw error))]
            {:scheduler scheduler
             :response (response request status-code)
             :event {:type :request/rejected :reason error-type :id key}}))))))

(defn- cancel [scheduler request]
  (let [key (request-key request)]
    (if (contains? (:active scheduler) key)
      {:scheduler (update scheduler :active dissoc key)
       :response (response request (:cancelled gs/status))
       :event {:type :request/cancelled :id key}}
      {:scheduler scheduler
       :event {:type :request/ignored :reason :unknown-cancel :id key}})))

(defn- update-extensions [scheduler request]
  (let [key (request-key request)]
    (if (contains? (:active scheduler) key)
      {:scheduler (update-in scheduler [:active key :extensions]
                             merge (:extensions request))
       :event {:type :request/updated :id key
               :extensions (set (keys (:extensions request)))}}
      {:scheduler scheduler
       :event {:type :request/ignored :reason :unknown-update :id key}})))

(defn handle-message
  "Handle the request side of one decoded GraphSync message in list order.
  Returns `{:scheduler :message? :events}`. Unknown cancel/update messages are
  ignored, matching go-graphsync response-manager behavior."
  [scheduler get-fn message traversal-limits]
  (when-not (= #{:requests} (set (keys message)))
    (invalid! "graphsync scheduler: responder accepts request-only messages"
              {:message-keys (set (keys message))}))
  (let [result
        (reduce
         (fn [{:keys [scheduler responses events]} request]
           (let [handled (case (:type request)
                           :new (admit-new scheduler get-fn request traversal-limits)
                           :cancel (cancel scheduler request)
                           :update (update-extensions scheduler request)
                           (invalid! "graphsync scheduler: unknown request type"
                                     {:request request}))]
             {:scheduler (:scheduler handled)
              :responses (cond-> responses (:response handled)
                           (conj (:response handled)))
              :events (conj events (:event handled))}))
         {:scheduler scheduler :responses [] :events []}
         (:requests message))]
    (cond-> {:scheduler (:scheduler result) :events (:events result)}
      (seq (:responses result))
      (assoc :message (result-message (:responses result))))))

(defn- scheduled-entry [scheduler]
  (first
   (sort-by (juxt (comp - :priority val) (comp :order val))
            (:active scheduler))))

(defn- block-chunk [scheduler blocks offset]
  (let [max-blocks (get-in scheduler [:config :max-blocks-per-message])
        max-bytes (get-in scheduler [:config :max-block-bytes-per-message])]
    (loop [index offset chunk [] bytes 0]
      (if (or (>= index (count blocks)) (>= (count chunk) max-blocks))
        chunk
        (let [block (nth blocks index)
              next-bytes (+ bytes (byte-length (:bytes block)))]
          (if (> next-bytes max-bytes)
            chunk
            (recur (inc index) (conj chunk block) next-bytes)))))))

(defn step
  "Emit at most one block chunk from the highest-priority active request.
  Equal priorities retain admission order. Returns nil message when idle."
  [scheduler]
  (if-let [[key {:keys [request blocks offset]}] (scheduled-entry scheduler)]
    (let [chunk (block-chunk scheduler blocks offset)]
      (when (empty? chunk)
        (invalid! "graphsync scheduler: no block fits configured message budget"
                  {:id key :offset offset}))
      (let [next-offset (+ offset (count chunk))
            terminal? (= next-offset (count blocks))
            status-code (if terminal?
                          (:completed-full gs/status)
                          (:partial-response gs/status))
            message {:responses [{:id (:id request)
                                  :status status-code
                                  :metadata (mapv (fn [{:keys [cid]}]
                                                    {:cid cid :action :present})
                                                  chunk)}]
                     :blocks chunk}
            scheduler (if terminal?
                        (update scheduler :active dissoc key)
                        (assoc-in scheduler [:active key :offset] next-offset))]
        {:scheduler scheduler
         :message message
         :event {:type (if terminal? :response/completed :response/partial)
                 :id key :blocks (count chunk)}}))
    {:scheduler scheduler :message nil :event {:type :scheduler/idle}}))

(defn drain
  "Step until idle, bounded by positive `max-messages`. Returns emitted
  messages and the final scheduler."
  [scheduler max-messages]
  (when-not (and (integer? max-messages) (pos? max-messages))
    (invalid! "graphsync scheduler: positive drain limit required"
              {:max-messages max-messages}))
  (loop [scheduler scheduler messages [] events [] remaining max-messages]
    (if (zero? (active-count scheduler))
      {:scheduler scheduler :messages messages :events events}
      (do
        (when (zero? remaining)
          (throw (ex-info "graphsync scheduler: drain limit exceeded"
                          {:type :graphsync/resource-limit
                           :limit :max-messages :maximum max-messages})))
        (let [result (step scheduler)]
          (recur (:scheduler result) (conj messages (:message result))
                 (conj events (:event result)) (dec remaining)))))))
