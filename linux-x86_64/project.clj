(defproject org.clojars.huahaiy/dtlvnative-linux-x86_64 "0.13.9"
  :description "Native dependency of Datalevin on Linux x86-64"
  :url "https://github.com/juji-io/dtlvnative"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.3"]
                 [org.bytedeco/javacpp "1.5.12"]]
  :java-source-paths ["../src/java"]
  :jar-exclusions [#"\.java"]
  :javac-options ["-Xlint:unchecked" "-Xlint:-options" "--release" "17"]
  :jvm-opts ["-Dorg.bytedeco.javacpp.logger.debug=false"]
  :main datalevin.dtlvnative.Test
  :deploy-repositories [["clojars" {:url           "https://repo.clojars.org"
                                    :username      :env/clojars_username
                                    :password      :env/clojars_password
                                    :sign-releases false}]])
