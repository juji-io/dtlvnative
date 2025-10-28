(def version "0.13.6")

(defproject org.clojars.huahaiy/dtlvnative-windows-x86_64 version
  :description "Native dependency of Datalevin database on Windows"
  :url "https://github.com/juji-io/dtlvnative"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.3"]
                 [org.bytedeco/javacpp "1.5.12"]]
  :java-source-paths ["../src/java"]
  :jar-exclusions [#"\.java"]
  :javac-options ["-Xlint:unchecked" "-Xlint:-options" "--release" "17"]
  :jvm-opts ["-Dorg.bytedeco.javacpp.logger.debug=false" "-Xcheck:jni" "-Dclojure.debug=false"]
  :main datalevin.dtlvnative.Test
  :deploy-repositories [["clojars" {:url           "https://repo.clojars.org"
                                    :username      :env/clojars_username
                                    :password      :env/clojars_password
                                    :sign-releases false}]])
