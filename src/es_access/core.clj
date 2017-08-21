(ns es-access.core
  (:require [korma.core :as k]
            [korma.db :refer [postgres create-db default-connection]]
            [clojure.data.json :as json]
            [clojure.walk :refer [keywordize-keys]])
  (:import [com.impossibl.postgres.jdbc PGDataSource]
           [com.impossibl.postgres.api.jdbc PGNotificationListener]))

(defonce database-state (atom nil))

(defn entity->clj [{:keys [creator type data version aggregate_id sequence_number]}]
  {:sequence-number sequence_number
   :creator (read-string creator)
   :type (read-string type)
   :version version
   :aggegate-id (when aggregate_id (read-string aggregate_id))
   :data (when data (keywordize-keys (json/read-str data)))})

(defmulti handle-event :type)
(defmethod handle-event :default [_])

(def listener
  (reify PGNotificationListener
    (^void notification [this ^int processId ^String channelName ^String payload]
     (-> payload
         json/read-str
         keywordize-keys
         entity->clj
         handle-event))))

(def host "eventstore")
(def port 5432)
(def db (System/getenv "EVENTSTORE_DB"))
(def user (System/getenv "EVENTSTORE_USER"))
(def password (System/getenv "EVENTSTORE_PASSWORD"))

(defn init-sql-connection []
  (println "initializing connection to eventstore")
  (let [db-spec (postgres {:host host
                           :port port
                           :db db
                           :user user
                           :password password
                           :delimiters ""})
        db (create-db db-spec)]
    (default-connection db)
    db))

(defn init-trigger []
  (println "initializing connection to eventstore")
  (let [ds (doto (PGDataSource.)
             (.setHost host)
             (.setPort port)
             (.setDatabase db)
             (.setUser user)
             (.setPassword password))
        conn (.getConnection ds)]
    (.addNotificationListener conn listener)
    (doto (.createStatement conn)
      (.execute "LISTEN new_event;")
      (.close))
    conn))

(defn start-db []
  (swap! database-state
         (fn [state]
           (if state state {:db (init-sql-connection)
                            :listener (init-trigger)}))))

(k/defentity events
  (k/table :events.events)
  (k/entity-fields :sequence_number :aggregate_id :creator :type :version :data)
  (k/transform entity->clj))

(defn all
  ([] (all 0))
  ([sequence-number]
   (k/select events (k/where {:sequence_number [> sequence-number]}))))

(defn by-type
  ([type] (by-type type 0))
  ([type sequence-number]
   (k/select events (k/where {:sequence_number [> sequence-number]
                              :type (str type)}))))

(defn by-aggregate [aggregate-id]
  (k/select events (k/where {:aggregate_id (pr-str aggregate-id)})))

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defn add! [{:keys [creator type data aggregate-id version]}]
  (k/insert events (k/values {:type (str type)
                              :version version
                              :creator (str creator)
                              :data (json/write-str data)
                              :aggregate_id (pr-str aggregate-id)})))
