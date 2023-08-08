(ns vis
  (:require
    [oz.core :as oz]
    [elo :refer :all]
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


; extract data for a few players and plot it out.
; need to work out who some interesting players are to plot.
; maybe search for highest elos over this period..
(def data-to-plot (apply tc/concat 
                         (mapv #(extract-player-elo elo-tab %) players-to-plot)))


(comment
  
  ; rendering plot using oz.
  (oz/start-server!)

  ; static version for readme.
  (def line-plot
    {:width  1000
     :height 400
     :data   {:values (dt/->vector (ds/rows data-to-plot))}
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
     :data      {:values (dt/->vector (ds/rows data-to-plot))}
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

  ;; Render the plot
  (oz/view! line-plot)
  )



