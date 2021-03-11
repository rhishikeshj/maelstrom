#!/usr/bin/env bb

(ns maelstrom.datomic
  (:gen-class)
  (:require
   [cheshire.core :as json]
   [clojure.walk :as walk]))


;;;;;;;;;;;;;;;;;;; Util functions ;;;;;;;;;;;;;;;;;;;


(defn intify-keys
  [m]
  (let [f (fn [[k v]]
            [(try
               (Integer/parseInt (name k))
               (catch Exception _
                 k)) v])]
    (walk/postwalk (fn [x]
                     (if (map? x)
                       (into {} (map f x))
                       x))
                   m)))

;;;;;; Input pre-processing functions ;;;;;;


(defn- read-stdin
  "Read a single line from stdin and return it"
  []
  (read-line))


(defn- process-stdin
  "Read lines from the stdin and calls the handler"
  [handler]
  (doseq [line (line-seq (java.io.BufferedReader. *in*))]
    (handler line)))


(defn- parse-json
  "Parse the received input as json"
  [input]
  (try
    (json/parse-string input true)
    (catch Exception e
      nil)))


;;;;;; Output Generating functions ;;;;;;

(defn- generate-json
  "Generate json string from input"
  [input]
  (when input
    (json/generate-string input)))


(let [l (Object.)]
  (defn- printerr
    "Print the received input to stderr"
    [input]
    (locking l
      (binding [*out* *err*]
        (println input)))))


(let [l (Object.)]
  (defn- printout
    "Print the received input to stdout"
    [input]
    (when input
      (locking l
        (println input)))))


(def node-id (atom ""))
(def node-nbrs (atom []))
(def next-message-id (atom 0))


(defn- reply
  ([src dest body]
   {:src src
    :dest dest
    :body body}))


(defn- send!
  ([input]
   (-> input
       generate-json
       printout))
  ([src dest body]
   (send! (reply src dest body))))


(defn- call-service
  [service body]
  (send! @node-id service body)
  ;; @WARN : This code assumes that when we
  ;; send a message to lin-kv (write to stdout)
  ;; the next line we get on stdin will be its reply
  ;; How does maelstrom do this ?? What if there are pending
  ;; inputs on stdin which we haven't processed ?
  (let [reply (read-stdin)]
    (parse-json reply)))


(defn- process-txns
  [txns]
  (mapv (fn [[f k v :as txn]]
          (case f
            "r"
            (let [db (->> {:type "read"
                           :key "root"}
                          (call-service "lin-kv")
                          :body
                          :value
                          intify-keys)]
              ["r" k (get db k)])

            "append"
            (let [from-db (->> {:type "read"
                                :key "root"}
                               (call-service "lin-kv")
                               :body
                               :value
                               intify-keys)
                  reply (->> {:type "cas"
                              :key "root"
                              :from from-db
                              :to (update from-db k (fnil conj []) v)
                              :create_if_not_exists true}
                             (call-service "lin-kv")
                             :body)]
              (if (= "cas_ok" (:type reply))
                txn
                (do
                  (printerr (str "CAS NOT OKAY!" reply))
                  nil)))))
        txns))


(defn- process-request
  [input]
  (let [body (:body input)
        r-body {:msg_id (swap! next-message-id inc)
                :in_reply_to (:msg_id body)}
        nid (:node_id body)
        nids (:node_ids body)]
    (case (:type body)
      "init"
      (do
        (reset! node-id nid)
        (reset! node-nbrs nids)
        (reply @node-id
               (:src input)
               (assoc r-body :type "init_ok")))

      "txn"
      (let [txns (process-txns (:txn body))]
        (reply @node-id
               (:src input)
               (assoc r-body
                      :txn txns
                      :type "txn_ok")))

      "replicate"
      nil)))


(defn -main
  "Read transactions from stdin and send output to stdout"
  []
  (process-stdin (comp printout
                       generate-json
                       process-request
                       parse-json)))


(-main)
