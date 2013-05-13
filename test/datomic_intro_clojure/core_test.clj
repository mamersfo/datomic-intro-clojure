(ns datomic-intro-clojure.core-test
  (:use clojure.test
        datomic-intro-clojure.solutions)
  (:require [datomic.api :as d]
            [clojure-csv.core :as csv]))

(def ^:dynamic *conn* nil)

(def uri "datomic:mem://soccer")

(defn create-and-connect []
  (println "Creating and connecting to database at" uri)
  (d/create-database uri)
  (d/connect uri))

(defn release-and-delete []
  (println "Releasing connection and deleting database at" uri)
  (d/release *conn*)
  (d/delete-database uri))

(defn load-datomic-file [filename]
  @(d/transact *conn* (read-string (slurp filename))))

(defn find-by-name [name]
	(let [query '[:find ?p :in $ ?n :where [?p :name ?n]]
		  result (d/q query (d/db *conn*) name)]
		(first (first result))))

(defn load-player-twitter-screen-name [filename]
  (let [rows (rest (csv/parse-csv (slurp filename)))]
	(doseq [r rows]
		(let [player-id (find-by-name (get r 0))
		      screen-name (get r 1)]
			@(d/transact *conn* [[:db/add player-id :player/twitter.screenName screen-name]])))))

(defn load-player-team-and-salary [filename]
  (let [rows (rest (csv/parse-csv (slurp filename)))]
	(doseq [r rows]
		(let [player-id (find-by-name (get r 0))
		      team-id (find-by-name (get r 1))
		      salary (read-string (get r 2))]
			@(d/transact *conn* [[:db/add player-id :player/team team-id]
                                 [:db/add player-id :player/salary salary]])))))	
	
(defn entity [e]
  (d/entity (d/db *conn*) (first e)))
	
(deftest entities-and-attributes
  (println "Exercise 1: find all entities")
  (testing "Failed entities-and-attributes test"
    (binding [*conn* (create-and-connect)]
      (load-datomic-file "data/schema-1.dtm")
      (load-datomic-file "data/data-1.dtm")
      ; task: define the query
      (let [query solution-1
            result (d/q query (d/db *conn*))]
        (doseq [r result] (println (entity r)))
        (release-and-delete)
        (is (= 153 (count result)))))))

(deftest specific-entities
  (println "Exercise 2: find all persons")
  (testing "Failed specific-entities test"
  (binding [*conn* (create-and-connect)]
    (load-datomic-file "data/schema-1.dtm")
    (load-datomic-file "data/data-1.dtm")
    ; task: define the query
    (let [query solution-2
          result (d/q query (d/db *conn*))]
        (doseq [r result] (println (entity r)))
        (release-and-delete)
        (is (= 85 (count result)))))))

(deftest aggregate-expressions
  (println "Exercise 3: find for each country the number of players and their average height")
  (testing "Failed aggregate-expressions test"
  (binding [*conn* (create-and-connect)]
    (load-datomic-file "data/schema-1.dtm")
    (load-datomic-file "data/data-1.dtm")
    ; task: define the query
    (let [query solution-3
          result (sort-by last > (d/q query (d/db *conn*)))
          found (first result)]
        (doseq [r result] (println r))
        (release-and-delete)
        (is (= 19       (count result)))
        (is (= "Sweden" (get found 0 )))
        (is (= 1        (get found 1)))
        (is (= 195.0    (get found 2)))))))

(deftest perform-joins
  (println "Exercise 4: find team name and salary for Zlatan")
  (testing "Failed perform-joins test"
  (binding [*conn* (create-and-connect)]
    (load-datomic-file "data/schema-1.dtm")
    (load-datomic-file "data/data-1.dtm")
    (load-datomic-file "data/schema-2.dtm")
    ; loading player team and salary data for 2011
    (load-player-team-and-salary "data/data-2-2011.csv")
    ; task: define the query
    (let [query solution-4
          result (d/q query (d/db *conn*) "Zlatan Ibrahimovic")
          found (first result)]
        (doseq [r result] (println r))
        (release-and-delete)
        (is (= 2          (count found)))
        (is (= "AC Milan" (get found 0 )))
        (is (= 9.0        (get found 1)))))))

(deftest time-travel
  (println "Exercise 5: find top earners for subsequent years")
  (testing "Failed time-travel test"
    (binding [*conn* (create-and-connect)]
      (load-datomic-file "data/schema-1.dtm")
      (load-datomic-file "data/data-1.dtm")
      (load-datomic-file "data/schema-2.dtm")
      ; loading player team and salary data for 2011
      (load-player-team-and-salary "data/data-2-2011.csv")
      ; find instant when salaries were first recorded
      (let [query '[:find ?instant :in $
                    :where [?p :player/salary _ ?tx]
                           [?tx :db/txInstant ?instant]]
            instant (first (first (d/q query (d/db *conn*))))]
        (println "salary data added on" instant)
        ; pause in order to discriminate between transactions
        (await-for 1000)
        ; loading player team and salary data for 2012
        (load-player-team-and-salary "data/data-2-2012.csv")
        (let [query '[:find ?name ?salary
                      :in $
                      :where [?player :name ?name]
                      [?player :player/salary ?salary]]
              result-2012 (sort-by last > (d/q query (d/db *conn*)))]
		  (is (= "Samuel Eto'o" (get (first result-2012) 0)))
		  ; task: change argument to get the facts for last year,
		  ; so that Cristiano Ronaldo turns out to be the top earner
		  (let [result-2011 (sort-by last > (d/q query (d/db *conn*)))]
			(release-and-delete)
			(is (= "Cristiano Ronaldo" (get (first result-2011) 0)))))))))

(deftest predicate-functions
  (println "Exercise 6: find a Twitter user's screenName and followersCount")
  (println "where followersCount is over one million followers")
  (testing "Failed predicate-functions test"
    (binding [*conn* (create-and-connect)]
      (load-datomic-file "data/schema-1.dtm")
      (load-datomic-file "data/data-1.dtm")
      (load-datomic-file "data/schema-3.dtm")
      (load-datomic-file "data/data-3.dtm")
      ; task: define the query
      (let [query solution-6
            result (d/q query (d/db *conn*))]
          (doseq [r result] (println r))
          (release-and-delete)
          (is (= 21 (count result)))))))

(deftest multiple-joins
  (println "Exercise 7: find names of players who are")
  (println "following Robin van Persie on Twitter")
  (testing "Failed multiple-joins test"
    (binding [*conn* (create-and-connect)]
      (load-datomic-file "data/schema-1.dtm")
      (load-datomic-file "data/data-1.dtm")
      (load-datomic-file "data/schema-3.dtm")
      (load-datomic-file "data/data-3.dtm")
      (load-datomic-file "data/schema-4.dtm")
      (load-player-twitter-screen-name "data/data-4.csv")
      ; task: define the query
      (let [query solution-7
            result (sort-by first compare (d/q query (d/db *conn*) "Robin van Persie"))]
          (doseq [r result] (println r))
          (release-and-delete)
          (is (= 23 (count result)))
          (is (= "Andrei Arshavin" (first (first result))))))))

(deftest using-rules
  (println "Exercise 8: find names of goalkeepers or defenders, using rules")
  (testing "Failed using-rules test"
    (binding [*conn* (create-and-connect)]
      (load-datomic-file "data/schema-1.dtm")
      (load-datomic-file "data/data-1.dtm")
      ; task: define the rules
      (let [query '[:find ?n :in $ % :where [?e :name ?n](goalkeepers_or_defenders ?e)]
            rules solution-8
            result (sort-by first compare (d/q query (d/db *conn*) rules))]
          (doseq [r result] (println r))
          (release-and-delete)
          (is (= 24 (count result)))))))