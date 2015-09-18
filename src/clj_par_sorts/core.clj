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

;; oops this should be tail recursive
(defn lst-merge
  [lst1 lst2]
  (cond
    (nil? lst1) lst2
    (nil? lst2) lst1
    :else (let
            [[f1 & rest1] lst1
             [f2 & rest2] lst2]
            (if (<= f1 f2)
              (cons f1 (lst-merge rest1 lst2))
              (cons f2 (lst-merge lst1 rest2))))))

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
;; the main thread "merges" the paritions, then picks the global partitions from
;; these samples, and sends these samples back to each sorting thread
;; each sorting thread bins it's values buy those samples, then sends the
;; samples to the appropriate thread
;; each thread receives a bunch of values and merges all the values it receives

;; all my merges here are (sort (flatten)) which is obviously incorrect, but im
;; not interested in figuring out the message passing
(defn sample-sort-thread
  [my-channel channel-to-threads nthreads size]
  (go
    (let
      [my-elements (sort (apply vector (take size (repeatedly #(rand-int 42)))))
       my-samples (take-nth (/ (count my-elements) nthreads) my-elements)]
      (do
        (>! my-channel my-samples)
        (let
          [global-partitioners (<! my-channel)
           parted              (partition-list my-elements global-partitioners)
           limited-channel     (a/take nthreads my-channel)]
          (do
            ;; shoot all the values everywhere (including self)
            (doall (map-indexed #(go (>! (get channel-to-threads %1) %2)) parted))
            ;; receive from everyone (including self)
            (let
              [result (<!! (a/reduce conj [] limited-channel))]
              (do
                (Thread/sleep (rand-int 200))
                (println (sort (flatten result)))))))))))

(defn sample-sort
  [n size]
  (let
    [channels  (apply vector (take n (repeatedly #(chan n))))]
    (do
      ;; HAVE TO force the evaluation to start the threads
      (doall (map #(sample-sort-thread % channels n size) channels))
      ;; start of "root thread"
      (let
        ;; should use merge instead of flatten + sort but this is okay for now
        [single-use-channel (a/merge (map #(a/take 1 %) channels))
         all-partitions (sort (flatten (<!! (a/reduce conj [] single-use-channel))))
         global-parts   (take-nth (/ (count all-partitions) n) all-partitions)]
        (do
          ;; send the partitions to each thread
          (doall (map #(>!! % global-parts) channels)))))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
