(defproject more.relational.tests "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [more.relational "0.1.0-SNAPSHOT"]
                 [criterium "0.4.3"]
                 [incanter "1.5.7"]]
  :main ^:skip-aot more.relational.tests
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :jvm-opts ["-Xmx10g"]
  )
