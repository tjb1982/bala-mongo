(defproject bala-mongo "0.1.2-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
                [org.clojure/clojure "1.6.0"]
                [marshal "1.0.0"]
                [org.mongodb/mongo-java-driver "2.12.3"]
		[org.apache.commons/commons-lang3 "3.3.2"]
                ]
  :main ^:skip-aot bala-mongo.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
