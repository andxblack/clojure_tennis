(ns tennis
  (:require [tech.v3.dataset :as ds]
            [tablecloth.api :as tc]
            [tech.v3.datatype.datetime :as dtype-dt]))
; use the whitelist to import only the columns we need.
; parse the date strings into proper dates.

(comment
  (mapv #(str "data/atp_matches_" % ".csv") (range 2000 2006))

  (def small
    (->
      (ds/->dataset "data/atp_matches_2000.csv"
                    {:column-whitelist ["tourney_date", "winner_id", "loser_id",
                                        "winner_rank", "loser_rank", "match_num",
                                        "tourney_id"]
                     :parser-fn        {"tourney_date" [:local-date "yyyyMMdd"]}
                     ; converts string names to keywords.
                     :key-fn           keyword})
      ; order by date and match number..
      (tc/order-by [:tourney_date :tourney_id :match_num])))

  (ds/concat small small)
  )


(defn my-order [ds]
  "wrap this to make it nice to use below."
  (tc/order-by ds [:tourney_date :tourney_id :match_num])
  )

(defn load-match-data
  [start-year end-year]
  (->> (mapv #(str "data/atp_matches_" % ".csv") (range start-year end-year))
       (mapv #(ds/->dataset %
                            {:column-whitelist ["tourney_date", "winner_id", "loser_id",
                                                "match_num", "tourney_id"]
                             :parser-fn        {"tourney_date" [:local-date "yyyyMMdd"]}
                             ; converts string names to keywords.
                             :key-fn           keyword}))
       ; order by date and match number..
       ; do this before concat, should be quicker.
       (mapv my-order)
       (apply ds/concat)
       )
  )

(comment
  (def bigger (time (load-match-data 1990 2005))))


(comment
  ; info on the data
  (tc/row-count small)
  (tc/info small :columns)
  (tc/column-names small)
  (ds/head small)

  (ds/head (ds/update-column small :tourney_date dtype-dt/datetime->epoch))

  )

; calculate some simple stuff.
; calculate prob that higher ranked player wins the match.
(comment
  ; function that takes a row and returns true if winner_rank is less than loser.
  ; my feeling is that there is a proper way of doing this by using a
  ; function over the two columns..
  (take 5 (small "winner_rank"))

  (type (small "winner_rank"))

  ; problem that some of the ranks are nil....
  (filter #(not (number? %)) (:winner_rank small))
  ; in this case winner rank is basically infinite.
  (> ##Inf 10)

  (tc/select-rows small #(not (number? (:winner_rank %))))

  (tc/select-rows small (comp #(< % 100) :winner_rank))


  (let [x (tc/select-rows small (range 10))]
    (< (x "winner_rank") (x "loser_rank"))
    )

  (small "winner_rank")

  (def DS (tc/dataset {:V1 (take 9 (cycle [1 2]))
                       :V2 (range 1 10)
                       :V3 (take 9 (cycle [0.5 1.0 1.5]))
                       :V4 (take 9 (cycle ["A" "B" "C"]))}))
  (tc/column-names DS  #{:int64 :float64} :datatype)

  (tc/map-columns DS
                  :sum-of-numbers
                  (tc/column-names DS  #{:int64 :float64} :datatype)
                  (fn [& rows]
                    (reduce + rows)))

  (tc/select-rows DS (comp #(< % 1) :V3))

  ; get s list of unique id for forming map

  (def initial-elos (zipmap id-list (repeat 1500)))
  ; may not even need to make distinct as this will happen when making map..

  ;even smaller dataset to mess with
  ; add columns for current elos, which will be updated.
  (def smaller (-> (ds/head small 10)
                   (tc/add-columns {:winner_elo 0.0 :loser_elo 0.0})))




  ; it's not obvious to me what the best way of doing this
  ; elo calculation is.
  ; fairly obvious to do so mutably, but harder here.
  ; Feel like the best way is to create the columns iterativly and then
  ; add them to the data frame once done.

  )


(defn elo-prob
  "given the ratings r1 and r2, calculate the probability of r1 winning."
  [r1 r2]
  (let [d-div-400 (/ (- r2 r1) 400)
        p (java.lang.Math/pow 10 d-div-400)]
    (/ 1 (+ 1 p)))
  )


(defn unique-id-list
  "Return all the unique player ids in a given dataset."
  [ds]
  (distinct (into (seq (:winner_id ds)) (seq (:loser_id ds))))
  )

(defn calc-elos
  "Iteratively calculate some elos for a set of matches `dataset`."
  [dataset]
  (let [id-list (unique-id-list dataset)]
    (loop [[x & y] (ds/rows dataset)
           winner-elo-list []
           loser-elo-list []
           win-prob []
           current-elo-map (zipmap id-list (repeat 1500.0))]
      (if (empty? x)
        ; once finished we add new columns to the dataset.
        (tc/add-columns dataset {:winner-elo winner-elo-list
                                 :loser-elo  loser-elo-list
                                 :win-prob   win-prob})
        (let [win-id (:winner_id x)
              los-id (:loser_id x)
              win-elo (get current-elo-map win-id)
              los-elo (get current-elo-map los-id)
              mu (elo-prob win-elo los-elo)
              ;_ (prn los-elo win-elo)
              deviation (* 20 (- 1 mu))
              r1-new (+ win-elo deviation)
              r2-new (- los-elo deviation)
              ]
          (recur y
                 (conj winner-elo-list r1-new)
                 (conj loser-elo-list r2-new)
                 (conj win-prob mu)
                 (assoc current-elo-map win-id r1-new
                                        los-id r2-new)))
        )
      )))

; go through a dataset of matches and find each player's highest elo.
; useful for plotting and finding the best players.
(defn highest-elos
  "Return a map of the highest elo for each player in a `dataset`
   Requires the elos to have already been calculated."
  [dataset]
  (let [id-list (unique-id-list dataset)]
  (loop [[x & y] (ds/rows dataset)
         high-elo (zipmap id-list (repeat 1500.0))]
    (if (empty? x)
      high-elo
      ; only need to consider winners as this is when elo goes up.
      (let [win-id (:winner_id x)
            winners-new-elo (:winner-elo x)
            highest-elo-so-far (get high-elo win-id)]
        (if (> winners-new-elo highest-elo-so-far)
          ; update
          (recur y
                 (assoc high-elo win-id winners-new-elo))
          ; nothing to be done.
          (recur y
                 high-elo)
          )
        )
      )))
  )

(defn player? [player-id]
  "returns a boolean function that works on a row to say if a certain
  player competed (won or lost) a match."
  (fn [row]
    (or (= player-id (:winner_id row))
        (= player-id (:loser_id row)))))

(defn elo-for-player [player-id]
  "Returns the elo for a player regardless of if they won or lost."
  (fn [row]
    (if
      (= player-id (:winner_id row))
      (:winner-elo row)
      (:loser-elo row)
      )))


(comment
  ; function that returns a boolean function for a given player id..
  (defn player? [row]
    ; probably a better way of getting these elements from my map.
    (or (= 103163 (:winner_id row))
        (= 103163 (:loser_id row))))

  (def isTommy? (player? 103163))

  (isTommy? {:winner_id 103161 :loser_id 103163})
  (player? {:winner_id 103161 :loser_id 103163})

  (tc/select-rows elo-tab isTommy?)


  (tc/write! elo-tab "small.csv")

  ((elo-for-player 103163) {} )

  ; if player is winner or loser, then grad the corresponding elo.
  (ds/row-map tommy (fn [row]
                      {:bum ((elo-for-player 103163) row)}))

  )

(defn extract-player-elo
  "Once elos are calculated, extract data for specific players for plotting"
  [ds-with-elo player-id]
  (->
    (tc/select-rows ds-with-elo (player? player-id))
    ; sorted by date and match number so last entry is when they win
    ; the tourney or crash out.
    ; eventually want to work out a way of getting all matches in a tourney.
    (tc/unique-by :tourney_date {:strategy :last})
    ; format the dates differently to plot nicely.
    (ds/update-column :tourney_date dtype-dt/datetime->epoch)
    (ds/row-map (fn [row]
                  {:elo ((elo-for-player player-id) row)}))
    ; return only the cols useful for plotting.
    (tc/select-columns [:tourney_date :elo])
    ; append new col of player id now we have got rid of others.
    (tc/add-column :player_id player-id)
    )
  )




(comment

  (def bigger (time (load-match-data 1990 2005)))
  (def elo-tab (calc-elos bigger))
  ; extract data for Tommy Hass
  (def tommy (extract-player-elo elo-tab 103163))

  (tc/head tommy)

  )