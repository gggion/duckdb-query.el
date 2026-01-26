;;; duckdb-query-bench.el --- Benchmark suite for duckdb-query -*- lexical-binding: t; -*-

;; Author: Gino Cornejo <gggion123@gmail.com>
;; Maintainer: Gino Cornejo <gggion123@gmail.com>
;; Homepage: https://github.com/gggion/duckdb-query.el

;; This file is part of duckdb-query.

;; SPDX-License-Identifier: GPL-3.0-or-later

;; This file is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published
;; by the Free Software Foundation, either version 3 of the License,
;; or (at your option) any later version.
;;
;; This file is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with this file.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; Benchmark tools for duckdb-query package performance analysis.
;;
;; Basic usage:
;;
;;     (duckdb-query-bench-runner)
;;     ;; Runs full benchmark suite over the specified FILE
;;
;;     (duckdb-query-bench-query "SELECT * FROM range(1000)")
;;     ;; Benchmarks specific query across all formats
;;
;; The package provides:
;; - `duckdb-query-bench-runner' - Full benchmark harness
;; - `duckdb-query-bench-query' - Ad-hoc query benchmarking
;; - `duckdb-query-bench-formats' - Format comparison
;; - `duckdb-query-bench-columnar-detailed' - Detailed columnar analysis
;;
;; Key commands:
;; - `duckdb-query-bench-runner' - Run complete benchmark suite
;; - `duckdb-query-bench-query' - Benchmark custom query


;;; Code:

(require 'duckdb-query)
(require 'cl-lib)
(require 'benchmark)

;;; Customization

(defgroup duckdb-query-bench nil
  "Benchmarking for `duckdb-query' package."
  :group 'duckdb-query
  :prefix "duckdb-query-bench-")

(defcustom duckdb-query-bench-iterations 5
  "Number of iterations for each benchmark measurement."
  :type 'integer
  :group 'duckdb-query-bench)

(defcustom duckdb-query-bench-default-rows 1000
  "Default row count for generated test queries."
  :type 'integer
  :group 'duckdb-query-bench)

;;; Infrastructure

(defun duckdb-query-bench--measure (iterations fn)
  "Run FN ITERATIONS times and return timing plist."
  (let ((times nil)
        (gc-cons-threshold most-positive-fixnum))
    (dotimes (_ iterations)
      (garbage-collect)
      (push (benchmark-elapse (funcall fn)) times))
    (list :mean (/ (apply #'+ times) (float iterations))
          :min (apply #'min times)
          :max (apply #'max times)
          :iterations iterations)))

(defun duckdb-query-bench--format-time (seconds)
  "Format SECONDS as human-readable string."
  (if (>= seconds 1.0)
      (format "%.3fs" seconds)
    (format "%.2fms" (* seconds 1000))))

(defun duckdb-query-bench--stats-to-row (label stats)
  "Convert STATS plist to table row with LABEL."
  (list label
        (duckdb-query-bench--format-time (plist-get stats :mean))
        (duckdb-query-bench--format-time (plist-get stats :min))
        (duckdb-query-bench--format-time (plist-get stats :max))
        (plist-get stats :iterations)))

(defun duckdb-query-bench--default-query (&optional rows)
  "Generate default benchmark query with ROWS rows."
  (let ((n (or rows duckdb-query-bench-default-rows)))
    (format "SELECT i as id, 'name_' || i as name, i * 1.5 as value FROM range(%d) t(i)" n)))

(defun duckdb-query-bench--generate-data (size)
  "Generate test alist data with SIZE rows."
  (let ((data nil))
    (dotimes (i size)
      (push (list (cons 'id i)
                  (cons 'name (format "name_%d" i))
                  (cons 'value (* i 1.5)))
            data))
    (nreverse data)))

(defun duckdb-query-bench--nested-query (&optional rows)
  "Generate query with nested STRUCT types, ROWS rows."
  (let ((n (or rows duckdb-query-bench-default-rows)))
    (format "SELECT i, {'x': i, 'y': i*2}::STRUCT(x INT, y INT) as point, [i, i+1, i+2] as arr FROM range(%d) t(i)" n)))

;;; Session Executor Benchmarks

(cl-defun duckdb-query-bench-executor-comparison
    (&optional query &key (iterations duckdb-query-bench-iterations) (queries-per-iteration 10))
  "Compare :cli vs :session executor for QUERY.

QUERY is SQL string to execute.  When nil, use simple SELECT query.
ITERATIONS is number of measurement rounds.
QUERIES-PER-ITERATION is number of queries per round.

Return org-table data with CLI times, session times, and speedup.
Measures total round time and per-query latency.

Use to quantify session executor benefit for your workload.

Also see `duckdb-query-bench-high-frequency' for sustained load testing."
  (let* ((query (or query "SELECT 42 AS answer"))
         (cli-times nil)
         (session-times nil))
    ;; Measure CLI executor
    (dotimes (_ iterations)
      (garbage-collect)
      (let ((start (current-time)))
        (dotimes (_ queries-per-iteration)
          (duckdb-query query :executor :cli))
        (push (float-time (time-subtract (current-time) start)) cli-times)))
    ;; Measure session executor
    (duckdb-query-with-transient-session
      (duckdb-query query) ; warmup
      (dotimes (_ iterations)
        (garbage-collect)
        (let ((start (current-time)))
          (dotimes (_ queries-per-iteration)
            (duckdb-query query))
          (push (float-time (time-subtract (current-time) start)) session-times))))
    ;; Compute statistics
    (let* ((cli-mean (/ (apply #'+ cli-times) (float iterations)))
           (session-mean (/ (apply #'+ session-times) (float iterations)))
           (cli-per-query (* 1000 (/ cli-mean queries-per-iteration)))
           (session-per-query (* 1000 (/ session-mean queries-per-iteration)))
           (speedup (/ cli-mean session-mean)))
      (list '(metric cli session speedup)
            (list "total (ms)"
                  (format "%.1f" (* 1000 cli-mean))
                  (format "%.1f" (* 1000 session-mean))
                  (format "%.1fx" speedup))
            (list "per-query (ms)"
                  (format "%.2f" cli-per-query)
                  (format "%.2f" session-per-query)
                  (format "%.1fx" speedup))
            (list "queries/sec"
                  (format "%.0f" (/ queries-per-iteration cli-mean))
                  (format "%.0f" (/ queries-per-iteration session-mean))
                  "")))))

(cl-defun duckdb-query-bench-high-frequency
    (&key (duration-seconds 5) (query "SELECT 42") session-name)
  "Measure sustained query throughput over DURATION-SECONDS.

QUERY is SQL string to execute repeatedly.
DURATION-SECONDS is how long to run the test.
SESSION-NAME when provided uses existing session; otherwise creates transient.

Return plist with:
  :total-queries   - Number of queries executed
  :queries-per-sec - Average throughput
  :latencies-ms    - Vector of individual query latencies
  :p50-ms          - Median latency
  :p95-ms          - 95th percentile latency
  :p99-ms          - 99th percentile latency
  :max-ms          - Maximum latency

Use for profiling real-time query workloads where consistent low latency
matters more than average performance.

Also see `duckdb-query-bench-executor-comparison' for CLI vs session."
  (let ((latencies nil)
        (executor-fn (when session-name
                       (lambda ()
                         (duckdb-query query :executor :session :name session-name)))))
    (if session-name
        ;; Use provided session
        (let ((end-time (time-add (current-time) (seconds-to-time duration-seconds))))
          (while (time-less-p (current-time) end-time)
            (let ((start (current-time)))
              (funcall executor-fn)
              (push (* 1000 (float-time (time-subtract (current-time) start)))
                    latencies))))
      ;; Create transient session
      (duckdb-query-with-transient-session
        (duckdb-query query) ; warmup
        (let ((end-time (time-add (current-time) (seconds-to-time duration-seconds))))
          (while (time-less-p (current-time) end-time)
            (let ((start (current-time)))
              (duckdb-query query)
              (push (* 1000 (float-time (time-subtract (current-time) start)))
                    latencies))))))
    ;; Compute percentiles
    (let* ((sorted (sort (vconcat latencies) #'<))
           (n (length sorted))
           (p50-idx (floor (* n 0.50)))
           (p95-idx (floor (* n 0.95)))
           (p99-idx (floor (* n 0.99))))
      (list :total-queries n
            :queries-per-sec (/ (float n) duration-seconds)
            :latencies-ms sorted
            :p50-ms (aref sorted p50-idx)
            :p95-ms (aref sorted p95-idx)
            :p99-ms (aref sorted p99-idx)
            :max-ms (aref sorted (1- n))))))

(cl-defun duckdb-query-bench-read-write-workload
    (&key (duration-seconds 5) (read-ratio 0.8) (table-rows 1000))
  "Benchmark mixed read/write workload simulating real application.

DURATION-SECONDS is test duration.
READ-RATIO is fraction of operations that are reads (0.0-1.0).
TABLE-ROWS is initial table size.

Simulates workload with:
- Reads: SELECT with random WHERE clause
- Writes: INSERT single row

Return plist with operation counts, throughput, and latency percentiles
broken down by operation type.

Use to profile workloads that combine frequent reads with occasional writes."
  (let ((read-latencies nil)
        (write-latencies nil)
        (next-id (1+ table-rows)))
    (duckdb-query-with-transient-session
      ;; Setup table
      (duckdb-query (format "CREATE TABLE bench_rw AS
                             SELECT i AS id, 'value_' || i AS data, random() AS score
                             FROM generate_series(1, %d) t(i)"
                            table-rows))
      ;; Run mixed workload
      (let ((end-time (time-add (current-time) (seconds-to-time duration-seconds))))
        (while (time-less-p (current-time) end-time)
          (let ((start (current-time))
                (is-read (< (random 100) (* 100 read-ratio))))
            (if is-read
                (progn
                  (duckdb-query (format "SELECT * FROM bench_rw WHERE id = %d"
                                        (1+ (random table-rows))))
                  (push (* 1000 (float-time (time-subtract (current-time) start)))
                        read-latencies))
              (progn
                (duckdb-query (format "INSERT INTO bench_rw VALUES (%d, 'new_%d', %f)"
                                      next-id next-id (/ (random 1000) 1000.0)))
                (cl-incf next-id)
                (push (* 1000 (float-time (time-subtract (current-time) start)))
                      write-latencies))))))
      ;; Compute statistics
      (let* ((read-sorted (sort (vconcat read-latencies) #'<))
             (write-sorted (sort (vconcat write-latencies) #'<))
             (read-n (length read-sorted))
             (write-n (length write-sorted))
             (total-n (+ read-n write-n)))
        (list :total-ops total-n
              :ops-per-sec (/ (float total-n) duration-seconds)
              :read-ops read-n
              :write-ops write-n
              :actual-read-ratio (/ (float read-n) total-n)
              :read-p50-ms (when (> read-n 0) (aref read-sorted (floor (* read-n 0.5))))
              :read-p95-ms (when (> read-n 0) (aref read-sorted (floor (* read-n 0.95))))
              :write-p50-ms (when (> write-n 0) (aref write-sorted (floor (* write-n 0.5))))
              :write-p95-ms (when (> write-n 0) (aref write-sorted (floor (* write-n 0.95)))))))))

(cl-defun duckdb-query-bench-batch-operations
    (&key (batch-sizes '(1 10 50 100 500)) (rows-per-batch 1000) (iterations 3))
  "Benchmark batch INSERT performance at various batch sizes.

BATCH-SIZES is list of rows per INSERT statement to test.
ROWS-PER-BATCH is total rows to insert per test.
ITERATIONS is measurement repetitions.

Return org-table comparing throughput at each batch size.

Use to find optimal batch size for bulk data loading operations."
  (let ((results nil))
    (dolist (batch-size batch-sizes)
      (let ((times nil)
            (batches (ceiling rows-per-batch batch-size)))
        (dotimes (_ iterations)
          (duckdb-query-with-transient-session
            (duckdb-query "CREATE TABLE batch_test (id INT, data VARCHAR)")
            (garbage-collect)
            (let ((start (current-time))
                  (id 0))
              (dotimes (_ batches)
                (let ((values (mapconcat
                               (lambda (_)
                                 (cl-incf id)
                                 (format "(%d, 'data_%d')" id id))
                               (number-sequence 1 (min batch-size (- rows-per-batch (- id 1))))
                               ", ")))
                  (duckdb-query (concat "INSERT INTO batch_test VALUES " values))))
              (push (float-time (time-subtract (current-time) start)) times))))
        (let ((mean-time (/ (apply #'+ times) (float iterations))))
          (push (list batch-size
                      (format "%.0f" (* 1000 mean-time))
                      (format "%.0f" (/ rows-per-batch mean-time)))
                results))))
    (cons '(batch-size time-ms rows-per-sec) (nreverse results))))

(cl-defun duckdb-query-bench-session-overhead (&key (iterations 20))
  "Measure session startup and teardown overhead.

ITERATIONS is number of session lifecycle measurements.

Return plist with:
  :startup-p50-ms  - Median startup time
  :startup-p95-ms  - 95th percentile startup time
  :teardown-p50-ms - Median teardown time
  :teardown-p95-ms - 95th percentile teardown time
  :total-p50-ms    - Median full lifecycle time

Use to understand fixed costs of session management."
  (let ((startup-times nil)
        (teardown-times nil))
    (dotimes (_ iterations)
      (garbage-collect)
      (let* ((name (format "bench-%s" (random 100000)))
             (start (current-time)))
        (duckdb-query-session-start name)
        (duckdb-query "SELECT 1" :executor :session :name name)
        (push (* 1000 (float-time (time-subtract (current-time) start)))
              startup-times)
        (let ((kill-start (current-time)))
          (duckdb-query-session-kill name)
          (push (* 1000 (float-time (time-subtract (current-time) kill-start)))
                teardown-times))))
    (let ((startup-sorted (sort (vconcat startup-times) #'<))
          (teardown-sorted (sort (vconcat teardown-times) #'<))
          (n iterations))
      (list :startup-p50-ms (aref startup-sorted (floor (* n 0.5)))
            :startup-p95-ms (aref startup-sorted (floor (* n 0.95)))
            :teardown-p50-ms (aref teardown-sorted (floor (* n 0.5)))
            :teardown-p95-ms (aref teardown-sorted (floor (* n 0.95)))
            :total-p50-ms (+ (aref startup-sorted (floor (* n 0.5)))
                             (aref teardown-sorted (floor (* n 0.5))))))))

(defun duckdb-query-bench-latency-histogram (latencies-ms &optional bucket-width)
  "Generate histogram data from LATENCIES-MS vector.

BUCKET-WIDTH is milliseconds per bucket (default 1).

Return org-table with bucket ranges and counts for visualization."
  (let* ((bucket-width (or bucket-width 1.0))
         (max-latency (seq-max latencies-ms))
         (buckets (make-hash-table :test 'equal))
         (n-buckets (1+ (floor (/ max-latency bucket-width)))))
    ;; Count into buckets
    (seq-doseq (lat latencies-ms)
      (let ((bucket (* bucket-width (floor (/ lat bucket-width)))))
        (puthash bucket (1+ (gethash bucket buckets 0)) buckets)))
    ;; Build result table
    (cons '(range-ms count percentage)
          (cl-loop for i from 0 below (min n-buckets 20)
                   for bucket = (* i bucket-width)
                   for count = (gethash bucket buckets 0)
                   for pct = (* 100.0 (/ count (float (length latencies-ms))))
                   collect (list (format "%.0f-%.0f" bucket (+ bucket bucket-width))
                                 count
                                 (format "%.1f%%" pct))))))

;;; Core Benchmarks

(cl-defun duckdb-query-bench-formats (&optional query &key (iterations duckdb-query-bench-iterations))
  "Benchmark QUERY across all output formats.

QUERY is SQL string to execute.  When nil, use generated test data
with `duckdb-query-bench-default-rows' rows.

ITERATIONS is number of repetitions per format.  Defaults to
`duckdb-query-bench-iterations'.

Return org-table data: header row followed by one row per format.
Each row contains format name, mean time, min time, max time, and
iteration count.

Use to verify format conversion overhead for your data.  Format
choice should be based on downstream usage since overhead is
typically negligible.

Also see `duckdb-query-bench-query' for comprehensive profiling."
  (let ((query (or query (duckdb-query-bench--default-query)))
        (formats '(:alist :plist :hash :vector :columnar :org-table))
        (rows nil))
    (dolist (fmt formats)
      (let ((stats (duckdb-query-bench--measure
                    iterations
                    (lambda () (duckdb-query query :format fmt)))))
        (push (duckdb-query-bench--stats-to-row fmt stats) rows)))
    (cons '(format mean min max n) (nreverse rows))))

(cl-defun duckdb-query-bench-output-strategies (&optional query &key (iterations duckdb-query-bench-iterations))
  "Benchmark QUERY comparing :file vs :pipe output strategies.

QUERY is SQL string to execute.  When nil, use generated test data.

ITERATIONS is number of repetitions per strategy.  Defaults to
`duckdb-query-bench-iterations'.

Return org-table data with rows for :file strategy, :pipe strategy,
and speedup ratio.

File mode writes results to temp file via COPY statement.  Pipe mode
streams JSON through stdout.  File mode typically wins at large result
sets (>5k rows) while pipe mode may win at small results (<1k rows)
due to temp file overhead.

Use with your actual queries to determine optimal strategy for your
typical result sizes.

Also see `duckdb-query' parameter `:output-via'."
  (let* ((query (or query (duckdb-query-bench--default-query)))
         (file-stats (duckdb-query-bench--measure
                      iterations
                      (lambda () (duckdb-query query :output-via :file))))
         (pipe-stats (duckdb-query-bench--measure
                      iterations
                      (lambda () (duckdb-query query :output-via :pipe))))
         (speedup (/ (plist-get pipe-stats :mean)
                     (plist-get file-stats :mean))))
    (list '(strategy mean min max n)
          (duckdb-query-bench--stats-to-row :file file-stats)
          (duckdb-query-bench--stats-to-row :pipe pipe-stats)
          (list :speedup (format "%.2fx" speedup) "" "" ""))))

(cl-defun duckdb-query-bench-nested-types (&optional query &key (iterations duckdb-query-bench-iterations))
  "Benchmark nested type handling for QUERY.

QUERY is SQL string returning STRUCT, LIST, MAP, or ARRAY columns.
When nil, use generated query with nested types.

ITERATIONS is number of repetitions per strategy.  Defaults to
`duckdb-query-bench-iterations'.

Return org-table data comparing three approaches:

  file (auto)    - COPY TO JSON serializes nested types correctly
  pipe (strings) - Nested types become string representations
  pipe+preserve  - Extra DESCRIBE query wraps nested columns

File mode handles nested types automatically without overhead.  The
pipe+preserve path incurs additional latency from schema introspection.

Use with queries returning nested types to determine if `:preserve-nested'
overhead matters for your workload.

Also see `duckdb-query' parameters `:output-via' and `:preserve-nested'."
  (let* ((query (or query (duckdb-query-bench--nested-query)))
         (file-stats (duckdb-query-bench--measure
                      iterations
                      (lambda () (duckdb-query query :output-via :file))))
         (pipe-stats (duckdb-query-bench--measure
                      iterations
                      (lambda () (duckdb-query query :output-via :pipe))))
         (preserve-stats (duckdb-query-bench--measure
                          iterations
                          (lambda () (duckdb-query query
                                                   :output-via :pipe
                                                   :preserve-nested t)))))
    (list '(strategy mean min max n)
          (duckdb-query-bench--stats-to-row "file (auto)" file-stats)
          (duckdb-query-bench--stats-to-row "pipe (strings)" pipe-stats)
          (duckdb-query-bench--stats-to-row "pipe+preserve" preserve-stats))))

(cl-defun duckdb-query-bench-data-formats (&optional data &key (iterations duckdb-query-bench-iterations))
  "Benchmark `:data' parameter serialization with DATA.

DATA is list of alists to serialize.  When nil, generate test data
with `duckdb-query-bench-default-rows' rows.

ITERATIONS is number of repetitions per format.  Defaults to
`duckdb-query-bench-iterations'.

Return org-table data comparing :json vs :csv serialization formats,
plus speedup ratio.

JSON serialization uses native `json-serialize' and is typically faster
than CSV generation.  JSON also preserves types better (booleans, nulls,
nested structures).

Use with your actual Elisp data structures to verify the default
`:data-format :json' is optimal for your workload.

Also see `duckdb-query' parameters `:data' and `:data-format'."
  (let* ((data (or data (duckdb-query-bench--generate-data duckdb-query-bench-default-rows)))
         (json-stats (duckdb-query-bench--measure
                      iterations
                      (lambda ()
                        (duckdb-query "SELECT COUNT(*) FROM @data"
                                      :data data
                                      :data-format :json))))
         (csv-stats (duckdb-query-bench--measure
                     iterations
                     (lambda ()
                       (duckdb-query "SELECT COUNT(*) FROM @data"
                                     :data data
                                     :data-format :csv))))
         (speedup (/ (plist-get csv-stats :mean)
                     (plist-get json-stats :mean))))
    (list '(format mean min max n)
          (duckdb-query-bench--stats-to-row :json json-stats)
          (duckdb-query-bench--stats-to-row :csv csv-stats)
          (list :json-speedup (format "%.2fx" speedup) "" "" ""))))

(cl-defun duckdb-query-bench-schema (&optional source &key (iterations duckdb-query-bench-iterations))
  "Benchmark schema introspection for SOURCE.

SOURCE is table name, file path, or SELECT query to describe.
When nil, use simple SELECT query.

ITERATIONS is number of repetitions per function.  Defaults to
`duckdb-query-bench-iterations'.

Return org-table data with timing for `duckdb-query-describe',
`duckdb-query-columns', and `duckdb-query-column-types'.

All three functions execute DESCRIBE internally; differences reflect
only post-processing overhead which is typically negligible.

Use with your actual data sources to measure introspection latency.

Also see `duckdb-query-describe', `duckdb-query-columns',
`duckdb-query-column-types'."
  (let* ((source (or source "SELECT 1 as a, 'x' as b, 3.14 as c"))
         (describe-stats (duckdb-query-bench--measure
                          iterations
                          (lambda () (duckdb-query-describe source))))
         (columns-stats (duckdb-query-bench--measure
                         iterations
                         (lambda () (duckdb-query-columns source))))
         (types-stats (duckdb-query-bench--measure
                       iterations
                       (lambda () (duckdb-query-column-types source)))))
    (list '(function mean min max n)
          (duckdb-query-bench--stats-to-row 'duckdb-query-describe describe-stats)
          (duckdb-query-bench--stats-to-row 'duckdb-query-columns columns-stats)
          (duckdb-query-bench--stats-to-row 'duckdb-query-column-types types-stats))))

(cl-defun duckdb-query-bench-extraction (&optional query &key (iterations duckdb-query-bench-iterations))
  "Benchmark extraction utilities for QUERY.

QUERY is SQL string returning single column for extraction tests.
When nil, use generated range query with `duckdb-query-bench-default-rows'
rows.

ITERATIONS is number of repetitions per function.  Defaults to
`duckdb-query-bench-iterations'.

Return org-table data with timing for `duckdb-query-value',
`duckdb-query-column' as list, and `duckdb-query-column' as vector.

The value extraction runs an aggregation query derived from QUERY.
Column extraction tests both list and vector return formats.

Use to measure extraction overhead for your typical column sizes.

Also see `duckdb-query-value', `duckdb-query-column'."
  (let* ((query (or query (format "SELECT i FROM range(%d) t(i)" duckdb-query-bench-default-rows)))
         (value-query (format "SELECT MAX(x) FROM (%s) t(x)" query))
         (value-stats (duckdb-query-bench--measure
                       iterations
                       (lambda () (duckdb-query-value value-query))))
         (col-list-stats (duckdb-query-bench--measure
                          iterations
                          (lambda () (duckdb-query-column query))))
         (col-vec-stats (duckdb-query-bench--measure
                         iterations
                         (lambda () (duckdb-query-column query :as-vector t)))))
    (list '(function mean min max n)
          (duckdb-query-bench--stats-to-row 'duckdb-query-value value-stats)
          (duckdb-query-bench--stats-to-row "column (list)" col-list-stats)
          (duckdb-query-bench--stats-to-row "column (vector)" col-vec-stats))))

;;; Comprehensive Benchmarks

(cl-defun duckdb-query-bench-query (&optional query &key (iterations duckdb-query-bench-iterations))
  "Comprehensive benchmark for QUERY.

QUERY is SQL string to profile.  When nil, use generated test data.

ITERATIONS is number of repetitions per test.  Defaults to
`duckdb-query-bench-iterations'.

Return org-table data with format comparison and output strategy
comparison in unified table.  Each row tagged with test category
\(format or output) and specific item being tested.

Use as one-stop profiling for any query you need to optimize.
Identifies both optimal format and optimal output strategy.

Also see `duckdb-query-bench-formats', `duckdb-query-bench-output-strategies'."
  (let ((query (or query (duckdb-query-bench--default-query)))
        (rows nil))

    ;; All formats
    (dolist (fmt '(:alist :plist :hash :vector :columnar :org-table))
      (let ((stats (duckdb-query-bench--measure
                    iterations
                    (lambda () (duckdb-query query :format fmt)))))
        (push (cons "format" (duckdb-query-bench--stats-to-row fmt stats)) rows)))

    ;; Output strategies
    (dolist (strategy '(:file :pipe))
      (let ((stats (duckdb-query-bench--measure
                    iterations
                    (lambda () (duckdb-query query :output-via strategy)))))
        (push (cons "output" (duckdb-query-bench--stats-to-row strategy stats)) rows)))

    (cons '(test item mean min max n) (nreverse rows))))

(cl-defun duckdb-query-bench-runner (&key
                                     queries
                                     (iterations duckdb-query-bench-iterations)
                                     (test-formats t)
                                     (test-output t)
                                     (test-data t)
                                     (test-nested t)
                                     (test-schema t)
                                     (test-extraction t)
                                     (test-session t))
  "Run benchmark suite on QUERIES.

QUERIES is list of (LABEL . QUERY) pairs where LABEL is display name
and QUERY is SQL string.  When nil, generate queries at 100, 1000,
and 10000 rows.

ITERATIONS is number of repetitions per test.  Defaults to
`duckdb-query-bench-iterations'.

Keyword arguments enable or disable test categories:

  TEST-FORMATS    - Format conversion benchmarks
  TEST-OUTPUT     - Output strategy benchmarks
  TEST-DATA       - Data injection benchmarks
  TEST-NESTED     - Nested type benchmarks
  TEST-SCHEMA     - Schema introspection benchmarks
  TEST-EXTRACTION - Extraction utility benchmarks
  TEST-SESSION    - Session executor benchmarks

Return org-table data with all benchmark results.  Each row tagged
with benchmark category and specific item tested.

Use for comprehensive performance characterization.  Disable categories
irrelevant to your workload for faster execution.

Also see individual benchmark functions for focused testing."
  (let ((queries (or queries
                     `(("100" . ,(duckdb-query-bench--default-query 100))
                       ("1k" . ,(duckdb-query-bench--default-query 1000))
                       ("10k" . ,(duckdb-query-bench--default-query 10000)))))
        (rows nil))

    ;; Format benchmarks
    (when test-formats
      (dolist (q queries)
        (let ((label (car q))
              (query (cdr q)))
          (dolist (fmt '(:alist :columnar :org-table))
            (let ((stats (duckdb-query-bench--measure
                          iterations
                          (lambda () (duckdb-query query :format fmt)))))
              (push (cons (format "format/%s" label)
                          (duckdb-query-bench--stats-to-row fmt stats))
                    rows))))))

    ;; Output strategy benchmarks
    (when test-output
      (dolist (q queries)
        (let ((label (car q))
              (query (cdr q)))
          (dolist (strategy '(:file :pipe))
            (let ((stats (duckdb-query-bench--measure
                          iterations
                          (lambda () (duckdb-query query :output-via strategy)))))
              (push (cons (format "output/%s" label)
                          (duckdb-query-bench--stats-to-row strategy stats))
                    rows))))))

    ;; Data injection benchmarks
    (when test-data
      (dolist (size '(100 1000))
        (let ((data (duckdb-query-bench--generate-data size)))
          (dolist (fmt '(:json :csv))
            (let ((stats (duckdb-query-bench--measure
                          iterations
                          (lambda ()
                            (duckdb-query "SELECT COUNT(*) FROM @data"
                                          :data data
                                          :data-format fmt)))))
              (push (cons (format "data/%d" size)
                          (duckdb-query-bench--stats-to-row fmt stats))
                    rows))))))

    ;; Nested type benchmarks
    (when test-nested
      (let ((query (duckdb-query-bench--nested-query)))
        (dolist (config '((:file nil "file")
                          (:pipe nil "pipe")
                          (:pipe t "pipe+preserve")))
          (let ((stats (duckdb-query-bench--measure
                        iterations
                        (lambda ()
                          (duckdb-query query
                                        :output-via (nth 0 config)
                                        :preserve-nested (nth 1 config))))))
            (push (cons "nested"
                        (duckdb-query-bench--stats-to-row (nth 2 config) stats))
                  rows)))))

    ;; Schema benchmarks
    (when test-schema
      (let ((stats (duckdb-query-bench--measure
                    iterations
                    (lambda () (duckdb-query-describe "SELECT 1 as x")))))
        (push (cons "schema"
                    (duckdb-query-bench--stats-to-row "describe" stats))
              rows)))

    ;; Extraction benchmarks
    (when test-extraction
      (let ((query "SELECT i FROM range(10000) t(i)"))
        (let ((stats (duckdb-query-bench--measure
                      iterations
                      (lambda () (duckdb-query-value "SELECT MAX(i) FROM range(10000) t(i)")))))
          (push (cons "extract"
                      (duckdb-query-bench--stats-to-row "value" stats))
                rows))
        (let ((stats (duckdb-query-bench--measure
                      iterations
                      (lambda () (duckdb-query-column query)))))
          (push (cons "extract"
                      (duckdb-query-bench--stats-to-row "column" stats))
                rows))))

    ;; Session executor benchmarks
    (when test-session
      ;; CLI vs Session comparison
      (let ((comparison (duckdb-query-bench-executor-comparison
                         "SELECT 42" :iterations iterations :queries-per-iteration 20)))
        (dolist (row (cdr comparison))
          (push (cons "session/comparison" row) rows)))

      ;; High-frequency latency
      (let ((hf-result (duckdb-query-bench-high-frequency :duration-seconds 2)))
        (push (cons "session/throughput"
                    (list "queries/sec"
                          (format "%.0f" (plist-get hf-result :queries-per-sec))
                          "" "" ""))
              rows)
        (push (cons "session/latency"
                    (list "p50/p95/p99 (ms)"
                          (format "%.2f/%.2f/%.2f"
                                  (plist-get hf-result :p50-ms)
                                  (plist-get hf-result :p95-ms)
                                  (plist-get hf-result :p99-ms))
                          "" "" ""))
              rows))

      ;; Session lifecycle overhead
      (let ((overhead (duckdb-query-bench-session-overhead :iterations 10)))
        (push (cons "session/lifecycle"
                    (list "startup+teardown (ms)"
                          (format "%.1f" (plist-get overhead :total-p50-ms))
                          "" "" ""))
              rows)))

    (cons '(benchmark category mean min max n) (nreverse rows))))

;;;;
(cl-defun duckdb-query-bench-data-serialization
    (&optional data &key
               (iterations duckdb-query-bench-iterations)
               (sizes '(100 1000 5000 10000))
               (query "SELECT COUNT(*) FROM @data"))
  "Benchmark data parameter serialization performance.

DATA is optional alist data to benchmark.  When provided, SIZES is
ignored and only DATA is tested.  When nil, generate test data at
each size in SIZES.

ITERATIONS is number of repetitions per test.
SIZES is list of row counts to test when DATA is nil.
QUERY is SQL to execute against the data.

Return org-table with columns:
  rows       - Number of rows in data
  json-ms    - Time with :data-format :json
  csv-ms     - Time with :data-format :csv
  json-qps   - Queries per second with JSON
  csv-qps    - Queries per second with CSV
  speedup    - JSON speedup over CSV

Measures complete roundtrip:
    serialization + file write + DuckDB read + query execution.

Use to determine if your data size benefits from
JSON (default) or if CSV is acceptable for compatibility.

Example with custom data:

  (let ((my-data (load-application-data)))
    (duckdb-query-bench-data-serialization my-data :iterations 5))

Example comparing sizes:

  (duckdb-query-bench-data-serialization nil :sizes \\='(1000 5000 10000 50000))

Also see `duckdb-query-bench-data-formats' for simpler comparison.
Also see `duckdb-query' parameters `:data' and `:data-format'."
  (let ((results nil))
    (if data
        ;; User provided data - benchmark it directly
        (let* ((row-count (length data))
               (json-times nil)
               (csv-times nil))
          (dotimes (_ iterations)
            (garbage-collect)
            (let ((start (current-time)))
              (duckdb-query query :data data :data-format :json)
              (push (float-time (time-subtract (current-time) start)) json-times)))
          (dotimes (_ iterations)
            (garbage-collect)
            (let ((start (current-time)))
              (duckdb-query query :data data :data-format :csv)
              (push (float-time (time-subtract (current-time) start)) csv-times)))
          (let ((json-mean (/ (apply #'+ json-times) (float iterations)))
                (csv-mean (/ (apply #'+ csv-times) (float iterations))))
            (push (list row-count
                        (format "%.1f" (* 1000 json-mean))
                        (format "%.1f" (* 1000 csv-mean))
                        (format "%.0f" (/ 1.0 json-mean))
                        (format "%.0f" (/ 1.0 csv-mean))
                        (format "%.2fx" (/ csv-mean json-mean)))
                  results)))
      ;; Generate data at each size
      (dolist (size sizes)
        (let ((test-data (duckdb-query-bench--generate-data size))
              (json-times nil)
              (csv-times nil))
          (dotimes (_ iterations)
            (garbage-collect)
            (let ((start (current-time)))
              (duckdb-query query :data test-data :data-format :json)
              (push (float-time (time-subtract (current-time) start)) json-times)))
          (dotimes (_ iterations)
            (garbage-collect)
            (let ((start (current-time)))
              (duckdb-query query :data test-data :data-format :csv)
              (push (float-time (time-subtract (current-time) start)) csv-times)))
          (let ((json-mean (/ (apply #'+ json-times) (float iterations)))
                (csv-mean (/ (apply #'+ csv-times) (float iterations))))
            (push (list size
                        (format "%.1f" (* 1000 json-mean))
                        (format "%.1f" (* 1000 csv-mean))
                        (format "%.0f" (/ 1.0 json-mean))
                        (format "%.0f" (/ 1.0 csv-mean))
                        (format "%.2fx" (/ csv-mean json-mean)))
                  results)))))
    (cons '(rows json-ms csv-ms json-qps csv-qps speedup) (nreverse results))))

(cl-defun duckdb-query-bench-data-complexity
    (&key (iterations duckdb-query-bench-iterations)
          (row-count 1000))
  "Benchmark data serialization with varying structure complexity.

ITERATIONS is number of repetitions per test.
ROW-COUNT is number of rows for each complexity level.

Tests three complexity levels:
  flat    - Simple columns (id, name, value)
  nested  - Includes nested alist (metadata struct)
  arrays  - Includes list values (tags array)

Return org-table comparing serialization time across complexities.

Use to understand how nested structures in your Elisp data affect
serialization performance.  JSON handles nested structures natively;
CSV requires prin1 serialization which doesn't roundtrip.

Also see `duckdb-query-bench-data-serialization' for size scaling."
  (let ((results nil))
    ;; Flat data
    (let ((flat-data (cl-loop for i from 1 to row-count
                              collect `((id . ,i)
                                        (name . ,(format "name_%d" i))
                                        (value . ,(* i 1.5))))))
      (let ((stats (duckdb-query-bench--measure
                    iterations
                    (lambda ()
                      (duckdb-query "SELECT COUNT(*) FROM @data"
                                    :data flat-data
                                    :data-format :json)))))
        (push (duckdb-query-bench--stats-to-row "flat" stats) results)))

    ;; Nested data (alist within alist)
    (let ((nested-data (cl-loop for i from 1 to row-count
                                collect `((id . ,i)
                                          (name . ,(format "name_%d" i))
                                          (metadata . ((created . ,(format "2024-%02d-%02d"
                                                                           (1+ (mod i 12))
                                                                           (1+ (mod i 28))))
                                                       (score . ,(mod i 100))
                                                       (active . ,(= 0 (mod i 2)))))))))
      (let ((stats (duckdb-query-bench--measure
                    iterations
                    (lambda ()
                      (duckdb-query "SELECT COUNT(*) FROM @data"
                                    :data nested-data
                                    :data-format :json)))))
        (push (duckdb-query-bench--stats-to-row "nested" stats) results)))

    ;; Array data (lists as values)
    (let ((array-data (cl-loop for i from 1 to row-count
                               collect `((id . ,i)
                                         (name . ,(format "name_%d" i))
                                         (tags . (,(format "tag_%d" (mod i 5))
                                                  ,(format "cat_%d" (mod i 3))
                                                  ,(format "grp_%d" (mod i 7))))))))
      (let ((stats (duckdb-query-bench--measure
                    iterations
                    (lambda ()
                      (duckdb-query "SELECT COUNT(*) FROM @data"
                                    :data array-data
                                    :data-format :json)))))
        (push (duckdb-query-bench--stats-to-row "arrays" stats) results)))

    (cons '(complexity mean min max n) (nreverse results))))


(provide 'duckdb-query-bench)

;;; duckdb-query-bench.el ends here
