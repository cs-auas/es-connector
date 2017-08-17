(ns es-access.core
  (:require [korma.core :refer [defentity table entity-fields select insert where values]]
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

(defentity events
  (table :events.events)
  (entity-fields :id :creator :type :version :data))

(defn entity->clj [{:keys [creator type data version]}]
  {:creator (read-string creator)
   :type (read-string type)
   :version version
   :data (keywordize-keys (json/read-str data))})

(defn entities->clj [events]
  (map entity->clj events))

(defn entities->json [events]
  (json/write-str events))

(defn all
  ([] (all 0))
  ([id]
   (select events (where {:id [> id]}))))

(defn by-type
  ([type] (by-type type 0))
  ([type id]
   (select events (where {:id [> id]
                          :type (str type)}))))

(defn add! [{:keys [creator type data] :as event}]
  (insert events (values (assoc event :type (str type) :creator (str creator) :data (json/write-str data)))))
