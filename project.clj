(defproject clj_par_sorts "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]]
  :main ^:skip-aot clj-par-sorts.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
