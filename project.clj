(defproject cs-auas/es-access "0.2.4"
  :description "Connector to our eventstore, probably you don't want this lib"
  :url "https://github.com/cs-auas/es-connector"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha17"]
                 [korma "0.4.3"]
                 [postgresql/postgresql "9.3-1102.jdbc41"]
                 [com.impossibl.pgjdbc-ng/pgjdbc-ng "0.7"]
                 [org.clojure/data.json "0.2.6"]])
