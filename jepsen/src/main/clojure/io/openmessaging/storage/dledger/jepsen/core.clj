(ns io.openmessaging.storage.dledger.jepsen.core
  (:require [clojure.string :as cstr]
            [clojure.tools.logging :refer [info]]
            [jepsen [cli :as cli]
             [control :as c]
             [db :as db]
             [tests :as tests]
             [checker :as checker]
             [client :as client]
             [generator :as gen]
             [nemesis :as nemesis]
             [os :as os]
             [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model])
  (:import [io.openmessaging.storage.dledger.example.register RegisterDLedgerClient]))

(defonce dledger-path "/root/dledger-jepsen")
(defonce dledger-port 20911)
(defonce dledger-bin "java")
(defonce dledger-start "startup.sh")
(defonce dledger-stop "stop.sh")
(defonce dledger-stop-dropcaches "stop_dropcaches.sh")
(defonce dledger-data-path "/tmp/dledgerstore")
(defonce dledger-log-path "logs/dledger")

(defn peer-id [node]
  (str node))

(defn peer-str [node]
  (str (peer-id node) "-" node ":" dledger-port))

(defn peers
  "Constructs an initial cluster string for a test, like
  \"n0-host1:20911;n1-host2:20911,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (peer-str node)))
       (cstr/join ";")))

(defn start! [test node]
  (info "Start DLedgerServer" node)
  (c/cd dledger-path
        (c/exec :sh
                dledger-start
                "--group jepsen"
                "--id"
                (peer-id node)
                "--peers"
                (peers test))))

(defn stop! [node]
  (info "Stop DLedgerServer" node)
  (c/cd dledger-path
        (c/exec :sh
                dledger-stop)))

(defn stop_dropcaches! [node]
  (info "Stop DLedgerServer and drop caches" node)
  (c/cd dledger-path
        (c/exec :sh
                dledger-stop)))

(defn- create-client [test]
  (doto (RegisterDLedgerClient. "jepsen" (peers test))
    (.startup)))

(defn- start-client [client]
  (-> client
      :conn
      (.startup)))

(defn- shutdown-client [client]
  (-> client
      :conn
      (.shutdown)))

(defn- write
  "write a key-value to DLedger"
  [client key value]
  (-> client :conn
      (.write key value)))

(defn- read
  "read a key-value from DLedger"
  [client key]
  (-> client :conn
      (.read key)))

(defn db
  "Regitser-Mode DLedger Server"
  []
  (reify db/DB
    (setup! [_ test node]
      (start! test node)
      (Thread/sleep 1000))

    (teardown! [_ _ node]
      (stop! node)
      (Thread/sleep 1000)
      (c/exec
       :rm
       :-rf
       dledger-data-path))))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (-> this (assoc :node node) (assoc :conn (create-client test))))

  (setup! [_ _])

  (invoke! [this _ op]
    (try
      (case (:f op)
        :write (let [code, (-> (write this 13 (:value op)) .getCode)]
                 (cond
                   (= code 200) (assoc op :type :ok)
                   :else (assoc op :type :fail :error (str "write failed with code " code))))

        :read (let [res, (read this 13)]
                (cond
                  (= (res .getCode) 200) (assoc op :type :ok :value [(res .getKey) (res .getValue)])
                  :else (assoc op :type :fail :error (str "read failed with code " (res .getCode))))))

      (catch Exception e
        (assoc op :type :info :error e))))

  (teardown! [_ _])

  (close! [this _]
    (shutdown-client this)))

(def nemesis-map
  {"partition-random-halves" (nemesis/partition-random-halves)
   "partition-random-nodes" (nemesis/partition-random-node)
   "partition-majorities-ring" (nemesis/partition-majorities-ring)})

(defn- parse-int [s]
  (Integer/parseInt s))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(def cli-opts
  "Additional command line options."
  [["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--nemesis NAME" "What nemesis should we run?"
    :default  "partition-random-halves"
    :validate [nemesis-map (cli/one-of nemesis-map)]]
   ["-i" "--interval TIME" "How long is the nemesis interval?"
    :default  15
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]])

(defn dledger-test
  [opts]
  (let [nemesis (get nemesis-map (:nemesis opts))]
    (merge tests/noop-test
           opts
           {:name      "dledger"
            :os        os/noop
            :db        (db)
            :client    (Client. nil)
            :ssh       {:username "root" :password "root" :strict-host-key-checking false}
            :nemesis   nemesis
            :checker   (checker/compose
                        {:perf (checker/perf)
                         :indep (independent/checker
                                 (checker/compose
                                  {:timeline (timeline/html)
                                   :linear (checker/linearizable
                                            {:model (model/register)})}))})
            :generator  (->> (independent/concurrent-generator
                              (:concurrency opts 5)
                              (range)
                              (fn []
                                (->> (gen/mix [r w])
                                     (gen/stagger (/ (:rate opts)))
                                     (gen/limit (:ops opts)))))
                             (gen/nemesis
                              (gen/seq (cycle [(gen/sleep (:interval opts))
                                               {:type :info, :f :start}
                                               (gen/sleep (:internal opts))
                                               {:type :info, :f :stop}])))

                             (gen/time-limit (:time-limit opts)))})))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn dledger-test
                                         :opt-spec cli-opts})) args))