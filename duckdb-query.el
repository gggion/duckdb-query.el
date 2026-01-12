;;; duckdb-query.el --- Execute DuckDB queries with transducers -*- lexical-binding: t; -*-
;;
;; Author: Gino Cornejo
;; Maintainer: Gino Cornejo <gggion123@gmail.com>
;; Homepage: https://github.com/gggion/duckdb-query
;; Keywords: data sql

;; Package-Version: 0.1.0
;; Package-Requires: ((emacs "28.1") (transducers "1.5.0"))

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

(require 'transducers)
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

(defun duckdb-query-execute-raw (query &optional database timeout)
  "Execute QUERY via DuckDB CLI and return raw JSON string.

QUERY is SQL string.
DATABASE is optional path to database file; nil uses in-memory.
TIMEOUT is seconds to wait; nil uses `duckdb-query-default-timeout'.

Returns raw JSON output string for parsing.
Signals error on non-zero exit code.

Called by `duckdb-query'.
Uses `duckdb-query-executable' for subprocess invocation."
  (let ((timeout (or timeout duckdb-query-default-timeout)))
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
                   exit-code (string-trim (buffer-string)))))))))

(cl-defun duckdb-query (query &key database timeout (format :alist))
  "Execute QUERY and return results in FORMAT.

QUERY is SQL string.
DATABASE is optional database file path.
TIMEOUT is optional timeout in seconds.
FORMAT is output structure, one of:
  :alist  - list of alists (default)
  :plist  - list of plists
  :hash   - list of hash-tables
  :vector - vector of alists

Returns nil for empty results.

Uses `duckdb-query-execute-raw' for execution.
Uses `json-parse-string' for C-level parsing.
Uses `duckdb-query-null-value' and `duckdb-query-false-value'
for null/false representation."
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
        (_
         (error "Unknown format: %s.  Valid: :alist :plist :hash :vector" format))))))

(provide 'duckdb-query)

;;; duckdb-query.el ends here
