(defproject org.clojars.huahaiy/dtlvnative-linux-arm64 "0.13.22"
  :description "Native dependency of Datalevin on Linux Arm64"
  :url "https://github.com/juji-io/dtlvnative"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.3"]
                 [org.bytedeco/javacpp "1.5.12"]]
  :java-source-paths ["../src/java"]
  :jar-exclusions [#"\.java"]
  :javac-options ["-Xlint:unchecked" "-Xlint:-options" "--release" "17"]
  :main datalevin.dtlvnative.Test
  :jvm-opts ["-XX:+IgnoreUnrecognizedVMOptions"
             "--enable-native-access=ALL-UNNAMED"
             "--add-opens=java.base/java.nio=ALL-UNNAMED"
             "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"]
  :deploy-repositories [["clojars" {:url           "https://repo.clojars.org"
                                    :username      :env/clojars_username
                                    :password      :env/clojars_password
                                    :sign-releases false}]])
