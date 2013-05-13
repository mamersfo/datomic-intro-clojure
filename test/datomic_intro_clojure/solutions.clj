(ns datomic-intro-clojure.solutions)

(def solution-1 '[:find ?p
                  :in $ 
                  :where [?p :name]])

(def solution-2 '[:find ?p 
                   :in $ 
                   :where [?p :person/height]])

(def solution-3 '[:find ?cn (count ?c) (avg ?h) 
                  :in $ 
                  :where [?p :person/height ?h]
                         [?p :country ?c]
                         [?c :name ?cn]])

(def solution-4 '[:find ?t ?s 
                  :in $ ?n 
                  :where [?p :name ?n]
                         [?p :player/salary ?s]
                         [?p :player/team ?team]
                         [?team :name ?t]])

(def solution-5 '[:find ?instant 
                  :in $ ?n 
                  :where [?p :player/salary _ ?tx]
                          [?tx :db/txInstant ?instant]])

; database argument should be: conn.db().asOf( year2011 )

(def solution-6 '[:find ?s ?c 
                  :in $ 
                  :where [?t :twitter/screenName ?s]
                         [?t :twitter/followersCount ?c]
                         [(> ?c 1000000)]])

(def solution-7 '[:find ?name 
                  :in $ ?a1 
                  :where [?p1 :name ?a1]
                         [?p1 :player/twitter.screenName ?s1]
                         [?tw :twitter/screenName ?s1]
                         [?tw :twitter/followers ?fs]
                         [?fs :twitter/screenName ?s2]
                         [?p2 :player/twitter.screenName ?s2]
                         [?p2 :name ?name]])

(def solution-8 '[[[goalkeepers_or_defenders ?e]
                   [?e :player/position :position/goalkeeper]]
                   [[goalkeepers_or_defenders ?e]
                   [?e :player/position :position/defender]]])
