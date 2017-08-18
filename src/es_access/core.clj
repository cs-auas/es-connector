(ns es-access.core
  (:require [korma.core :as k]
            [korma.db :refer [postgres defdb]]
            [clojure.data.json :as json]
            [clojure.walk :refer [keywordize-keys]]))

(defn connect [host port db user password]
  (defdb pg (postgres
             {:host host
              :port port
              :db db
              :user user
              :password password
              :delimiters ""})))

(defn entity->clj [{:keys [creator type data version aggregate_id sequence_number]}]
  {:sequence-number sequence_number
   :creator (read-string creator)
   :type (read-string type)
   :version version
   :aggegate-id (when aggregate_id (read-string aggregate_id))
   :data (when data (keywordize-keys (json/read-str data)))})

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

(defn add! [{:keys [creator type data aggregate-id version]}]
  (k/insert events (k/values {:type (str type)
                              :version version
                              :creator (str creator)
                              :data (json/write-str data)
                              :aggregate_id (pr-str aggregate-id)})))


