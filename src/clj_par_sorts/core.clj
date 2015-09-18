(ns clj-par-sorts.core
  (:gen-class)
  (:require  [clojure.core.async
              :as a
              :refer  [>! <! >!! <!! go chan buffer close! thread
                       alts! alts!! timeout]]))

;; merge sort with futures
;; (time (def a (merge-sort        (take 8000 (repeatedly #(rand-int 42))))))
;; (time (def a (merge-sort-serial (take 8000 (repeatedly #(rand-int 42))))))
(declare merge-sort-parr)
(declare merge-sort-serial)
(declare merge-sort)

(defn lmerge
  [acc lists]
  (if (every? empty? lists)
    acc
    (let
      [ordered            (filter seq (sort-by first lists))
       list-with-smallest (first ordered)
       smallest           (first list-with-smallest)]
      (recur
        (conj acc smallest)
        (cons (rest list-with-smallest) (rest ordered))))))

(defn lst-merge [& args] (lmerge [] args))

(defn merge-sort-serial
  [lst]
  (if (<= (count lst) 1)
    lst
    (let
      [sides (split-at (/ (count lst) 2) lst)
       left  (merge-sort-serial (get sides 0))
       right (merge-sort-serial (get sides 1))]
      (lst-merge left right))))

(defn merge-sort
  [lst]
  (if (<= (count lst) 2000)
    (merge-sort-serial lst)
    (merge-sort-parr   lst)))

(defn merge-sort-parr
  [lst]
  (do
    (println "parallel sort called")
    (if (<= (count lst) 1)
      lst
      (let
        [sides (split-at (/ (count lst) 2) lst)
         left  (future (merge-sort (get sides 0)))
         right (future (merge-sort (get sides 1)))]
        (lst-merge @left @right)))))

;; sample sort
;; not really a good way to write a serial version of this for comparison

;; partitions must be sorted
;; this stuff sort of works
(defn which-partition
  [value partitions]
  (let
    [idxed-parts (map-indexed #(vector %1 %2) partitions)
     reducer (fn [kept element] (if (> (second element) value)
                                  (reduced kept)
                                  (first element)))]
    (reduce reducer '(0 0) idxed-parts)))

(defn partition-list
  [lst partitions]
  (let
    [init (repeat (count partitions) [])]
    (reduce
      (fn [parts element]
        (map-indexed
          #(if (= (which-partition element partitions) %1)
             (conj %2 element)
             %2)
          parts))
      init
      lst)))

;; thread generates some random elements
;; sorts its private data
;; samples nthreads elements from the list, and sends the samples back to the main
;; thread
;; the main thread merges the paritions, then picks the global partitions from
;; these samples, and sends these samples back to each sorting thread
;; each sorting thread bins it's values buy those samples, then sends the
;; samples to the appropriate thread
;; each thread receives a bunch of values and merges all the values it receives
;; im not sure where the stack overflow is occurring
(defn sample-sort-thread
  [my-channel root-channel channel-to-threads nthreads size]
  (go
    (let
      [my-elements (sort (take size (repeatedly #(rand-int (* 2 size)))))
       my-samples  (take-nth (/ (count my-elements) nthreads) my-elements)]
      (do
        (>! root-channel my-samples)
        (let
          [global-partitioners (<!! my-channel)
           parted              (partition-list my-elements global-partitioners)]
          (do
            ;; shoot all the values everywhere (including self)
            (doall (map-indexed #(go (>! (get channel-to-threads %1) %2)) parted))
            ;; receive from everyone (including self)
            (let
              [result (<!! (a/reduce conj [] (a/take nthreads my-channel)))]
              (do
                (>!! root-channel (apply lst-merge result))))))))))

(defn sample-sort
  [n size]
  (let
    [root-channel (chan n)
     channels     (vec (take n (repeatedly #(chan n))))]
    (do
      ;; HAVE TO force the evaluation to start the threads
      (doall (map #(sample-sort-thread % root-channel channels n size) channels))
      ;; start of "root thread"
      (let
        [single-use-channel (a/take n root-channel)
         all-partitions     (apply lst-merge (<!! (a/reduce conj [] single-use-channel)))
         global-parts       (take-nth (/ (count all-partitions) n) all-partitions)]
        (do
          (doall (map #(>!! % global-parts) channels))
          (let
            [res-chanel (a/take n root-channel)
             res        (apply lst-merge (<!! (a/reduce conj [] res-chanel)))]
            (println (and
                       (= (* n size) (count res))
                       (apply <= res)))))))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (sample-sort 1 100000))
