(ns vis
  (:require
    [oz.core :as oz]
    [tennis :refer :all]
    [tech.v3.dataset :as ds]
    [tech.v3.datatype :as dt]
    [tablecloth.api :as tc])
  )

(def players (ds/->dataset "data/atp_players.csv"
                           {:column-whitelist ["player_id" "name_last"]
                            :key-fn           keyword}))

(def bigger (time (load-match-data 1990 2005)))
(def elo-tab (calc-elos bigger))

; return a map of the highest elos for each player id, then find the 10
; highest and the corresponding player ids.
(def players-to-plot (->> elo-tab
                          (highest-elos)
                          (sort-by last)
                          (take-last 10)
                          ; the player ids are the first element.
                          (mapv first)))

(defn player-id->row-index [player-id]
  "convert an id to row index for fast lookup of player names."
  (dec (- player-id 100000)))

(def best-players
  (tc/select-rows players (map player-id->row-index players-to-plot)))



(comment

  ; f ilter the rows for my best players .
  (tc/select-rows players (fn [row] (= 103163 (:player_id row))))

  ; unique players in this set.
  (def u-players (unique-id-list elo-tab))

  ; for each player find their highest elo rating..
  ; easiest to just modeify the elo algo so that it also stores the max for each
  ; player as it goes.

  ;(def all (apply tc/concat
  ;                (mapv #(extract-player-elo elo-tab %) u-players)))

  (def tommy (extract-player-elo elo-tab 103166)))



; extract data for a few players and plot it out.
; need to work out who some interesting players are to plot.
; maybe search for highest elos over this period..
(def bum (apply tc/concat
                (mapv #(extract-player-elo elo-tab %) players-to-plot)))

; DONE next step is to extra and plot data for multiple players.
; will need to update the extract-player-elo to return something simpler
; just date, player-id and elo... should then just be able to map over
; id's then vega can colour by id...

; TODO now what? algo is working and we can plot stuff....
; 1. try optimising k,

(comment
  ; for serving clerk notebooks.
  (clerk/serve! {:browse? true})
  (clerk/show! "src/notebook1.clj")
  (clerk/serve! {:watch-paths ["notebooks" "src"]})
  )



(comment

  (defn play-data [& names]
    (for [n names
          i (range 20)]
      {:time i :item n :quantity (+ (Math/pow (* i (count n)) 0.8) (rand-int (count n)))}))

  (def data (list (doall (ds/mapseq-reader tommy))))
  (def tit (dt/->vector (ds/rows tommy)))

  )

(comment
  ; rendering plot using oz.
  (oz/start-server!)

  ; should be able to create a selector dropdown for which players to show.
  (def line-plot
    {:width  1000
     :height 400
     :data   {:values (dt/->vector (ds/rows bum))}
     ; use this to lookup last name using player id.
     :transform [{:lookup "player_id"
                   :from {:data {:values (dt/->vector (ds/rows best-players))},
                          :key "player_id",
                          :fields  ["name_last"]
                          }
                   }]
     :encoding {:x     {:field "tourney_date"
                        :type  "temporal"
                        :title "date"}
                :y     {:field "elo"
                        :type  "quantitative"
                        :scale {:domain [1450 2000]}}
                :color {:field "name_last"
                        :type  "nominal"}
                }
     :mark {:type        "line"
            :interpolate "step-after"}})

  ; interactive version.
  (def line-plot
    {:width     1000
     :height    400
     :data      {:values (dt/->vector (ds/rows bum))}
     ; use this to lookup last name using player id.
     :params    [{:name   "player",
                  :select {:type "point", :fields ["name_last"]},
                  :bind   {:input "select", :options (dt/->vector (:name_last best-players))}
                  }],
     :transform [{:lookup "player_id"
                  :from   {:data   {:values (dt/->vector (ds/rows best-players))},
                           :key    "player_id",
                           :fields ["name_last"]
                           }
                  }]
     :encoding  {:x     {:field "tourney_date"
                         :type  "temporal"
                         :title "date"}
                 :y     {:field "elo"
                         :type  "quantitative"
                         :scale {:domain [1450 2000]}}
                 :color {:condition {
                                     :param "player",
                                     :field "name_last"
                                     :type  "nominal"},

                         :value     "grey"}
                 }
     :mark      {:type        "line"
                 :interpolate "step-after"}})

  ;; Render the
  (oz/view! line-plot)
  )



