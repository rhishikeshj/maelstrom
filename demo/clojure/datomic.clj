#!/usr/bin/env bb

(ns maelstrom.datomic
  (:gen-class)
  (:require
    [cheshire.core :as json]))


;;;;;;;;;;;;;;;;;;; Util functions ;;;;;;;;;;;;;;;;;;;

;;;;;; Input pre-processing functions ;;;;;;


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
(def state (atom {}))


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


(defn- replicate-loop
  []
  (future
    (try
      (loop []
        (doseq [n @node-nbrs]
          (send! (reply @node-id n {:type "replicate"})))
        (Thread/sleep 5000)
        (recur))
      (catch Exception e
        (printerr e)))))


(defn- process-txns
  [txns]
  (mapv (fn [[f k v :as txn]]
          (case f
            "r"
            ["r" k (get @state k)]

            "append"
            (do
              (swap! state update k (fnil conj []) v)
              txn)))
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
  ;;(replicate-loop)
  (process-stdin (comp printout
                       generate-json
                       process-request
                       parse-json)))


(-main)
