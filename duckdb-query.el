;;; duckdb-query.el --- Execute DuckDB queries with transducers -*- lexical-binding: t; -*-
;;
;; Author: Gino Cornejo
;; Maintainer: Gino Cornejo <gggion123@gmail.com>
;; Homepage: https://github.com/gggion/duckdb-query
;; Keywords: data sql

;; Package-Version: 0.1.0
;; Package-Requires: ((emacs "28.1"))

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

;; Execute SQL queries against DuckDB and process results with transducers.
;;
;; Basic usage:
;;
;;     (duckdb-query "SELECT 42 as answer, 'hello' as greeting")
;;     ;; => ((("answer" . "42") ("greeting" . "hello")))
;;
;;     (duckdb-query "SELECT 42 as number" :transform-numbers t)
;;     ;; => ((("number" . 42)))
;;
;; The package provides:
;; - `duckdb-query' - Main query interface
;; - `duckdb-query-execute-raw' - Low-level execution
;; - `duckdb-query-parse-line-output' - Result parsing
;; - `duckdb-query-parse-numbers' - Number conversion transducer

;;; Code:

(require 'cl-lib)

;;; Customization

(defgroup duckdb-query nil
  "Execute DuckDB queries with transducers."
  :group 'data
  :prefix "duckdb-query-")

(defcustom duckdb-query-executable "duckdb"
  "Path to DuckDB executable.

Used by `duckdb-query-execute-raw'."
  :type 'string
  :group 'duckdb-query)

(defcustom duckdb-query-default-timeout 30
  "Default timeout in seconds for query execution.

Used by `duckdb-query-execute-raw' when TIMEOUT argument is nil."
  :type 'integer
  :group 'duckdb-query)

(defcustom duckdb-query-null-value :null
  "Value representing SQL NULL in query results.

Used by `duckdb-query' when parsing JSON output from DuckDB."
  :type '(choice (const :tag "Keyword :null" :null)
                 (const :tag "Symbol nil" nil)
                 (const :tag "String \"NULL\"" "NULL"))
  :group 'duckdb-query)

(defcustom duckdb-query-false-value :false
  "Value representing SQL FALSE in query results.

Used by `duckdb-query' when parsing JSON output from DuckDB."
  :type '(choice (const :tag "Keyword :false" :false)
                 (const :tag "Symbol nil" nil))
  :group 'duckdb-query)

;;; Core Functions

(defun duckdb-query-execute-raw (query &optional database _timeout)
  "Execute QUERY via DuckDB CLI and return raw JSON string.

QUERY is SQL string.
DATABASE is optional path to database file; nil uses in-memory.
_TIMEOUT is reserved for future use; currently ignored.

Returns raw JSON output string for parsing.
Signals error on non-zero exit code.

Called by `duckdb-query'.
Uses `duckdb-query-executable' for subprocess invocation."
  (with-temp-buffer
    (let* ((cmd (if database
                    (list duckdb-query-executable database "-json" "-c" query)
                  (list duckdb-query-executable "-json" "-c" query)))
           (default-directory temporary-file-directory)
           (process-environment (cons "DUCKDB_NO_COLOR=1" process-environment)))
      (let ((exit-code (apply #'call-process (car cmd) nil t nil (cdr cmd))))
        (if (zerop exit-code)
            (buffer-string)
          (error "DuckDB execution failed (exit %d): %s"
                 exit-code (string-trim (buffer-string))))))))

(defun duckdb-query--to-columnar (rows)
  "Convert ROWS to columnar format.

ROWS is list of alists from JSON parsing.

Returns alist of (COLUMN-NAME . VECTOR) pairs where each vector
contains all values for that column.

Uses hash table for O(1) column lookup and single-pass algorithm
to minimize cache misses.

Called by `duckdb-query' when FORMAT is `:columnar'."
  (when rows
    (let* ((row-count (length rows))
           (columns (mapcar #'car (car rows)))
           (col-count (length columns))
           (col-index (cl-loop with ht = (make-hash-table :test 'equal :size col-count)
                               for col in columns
                               for idx from 0
                               do (puthash col idx ht)
                               finally return ht))
           (col-vectors (make-vector col-count nil)))

      ;; Initialize column vectors
      (dotimes (i col-count)
        (aset col-vectors i (make-vector row-count nil)))

      ;; Fill vectors in single pass
      (cl-loop for row in rows
               for row-idx from 0
               do (dolist (cell row)
                    (when-let ((col-idx (gethash (car cell) col-index)))
                      (aset (aref col-vectors col-idx) row-idx (cdr cell)))))

      ;; Build result alist
      (cl-loop for col in columns
               for col-idx from 0
               collect (cons col (aref col-vectors col-idx))))))

(defun duckdb-query--to-org-table (rows)
  "Convert ROWS to org-table format.

ROWS is list of alists from JSON parsing.

Returns list of lists: first row is headers, remaining rows are values.
Each row is a list of column values in consistent order.

Called by `duckdb-query' when FORMAT is `:org-table'."
  (when rows
    (let* ((first-row (car rows))
           (headers (mapcar #'car first-row)))
      (cons headers
            (mapcar (lambda (row)
                      (mapcar (lambda (col)
                                (let ((val (cdr (assoc col row))))
                                  ;; Convert to string for org-table
                                  (if (stringp val)
                                      val
                                    (prin1-to-string val))))
                              headers))
                    rows)))))

(cl-defun duckdb-query (query &key database timeout (format :alist))
  "Execute QUERY and return results in FORMAT.

QUERY is SQL string.
DATABASE is optional database file path.
TIMEOUT is reserved for future timeout implementation; currently ignored.
FORMAT is output structure, one of:
  :alist     - list of alists (default)
  :plist     - list of plists
  :hash      - list of hash-tables
  :vector    - vector of alists
  :columnar  - alist of column vectors
  :org-table - list of lists for org-mode tables

Returns nil for empty results.

Uses `duckdb-query-execute-raw' for execution.
Uses `json-parse-string' for C-level parsing.
Uses `duckdb-query-null-value' and `duckdb-query-false-value'
for null/false representation.
Uses `duckdb-query--to-columnar' for columnar conversion.
Uses `duckdb-query--to-org-table' for org-table conversion."
  (let ((json-output (duckdb-query-execute-raw query database timeout)))
    (when (and json-output (not (string-empty-p (string-trim json-output))))
      (pcase format
        (:alist
         (json-parse-string json-output
                            :object-type 'alist
                            :array-type 'list
                            :null-object duckdb-query-null-value
                            :false-object duckdb-query-false-value))
        (:plist
         (json-parse-string json-output
                            :object-type 'plist
                            :array-type 'list
                            :null-object duckdb-query-null-value
                            :false-object duckdb-query-false-value))
        (:hash
         (json-parse-string json-output
                            :object-type 'hash-table
                            :array-type 'list
                            :null-object duckdb-query-null-value
                            :false-object duckdb-query-false-value))
        (:vector
         (json-parse-string json-output
                            :object-type 'alist
                            :array-type 'array
                            :null-object duckdb-query-null-value
                            :false-object duckdb-query-false-value))
        (:columnar
         (duckdb-query--to-columnar
          (json-parse-string json-output
                             :object-type 'alist
                             :array-type 'list
                             :null-object duckdb-query-null-value
                             :false-object duckdb-query-false-value)))
        (:org-table
         (duckdb-query--to-org-table
          (json-parse-string json-output
                             :object-type 'alist
                             :array-type 'list
                             :null-object duckdb-query-null-value
                             :false-object duckdb-query-false-value)))
        (_
         (error "Unknown format: %s.  Valid: :alist :plist :hash :vector :columnar :org-table" format))))))

(provide 'duckdb-query)

;;; duckdb-query.el ends here
