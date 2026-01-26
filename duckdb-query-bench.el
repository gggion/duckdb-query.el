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
                                     (test-extraction t))
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

    (cons '(benchmark category mean min max n) (nreverse rows))))

(provide 'duckdb-query-bench)

;;; duckdb-query-bench.el ends here
