;;; duckdb-query.el --- DuckDB query results as native Elisp data structures -*- lexical-binding: t; -*-
;;
;; Author: Gino Cornejo <gggion123@gmail.com>
;; Maintainer: Gino Cornejo <gggion123@gmail.com>
;; Homepage: https://github.com/gggion/duckdb-query.el
;; Keywords: data sql

;; Package-Version: 0.7.0
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

;; Execute DuckDB queries and return results as Elisp data structures.
;;
;; Basic usage:
;;
;;     (duckdb-query "SELECT 42 as answer, 'hello' as greeting")
;;     ;; => (((answer . 42) (greeting . "hello")))
;;
;;     (duckdb-query "SELECT * FROM 'data.csv'" :format :columnar)
;;     ;; => ((id . [1 2 3]) (name . ["Alice" "Bob" "Carol"]))
;;
;; Database context management:
;;
;;     (duckdb-query-with-database "app.db"
;;       (duckdb-query "SELECT * FROM users"))
;;
;; The package provides:
;; - `duckdb-query' - Execute queries with format conversion
;; - `duckdb-query-value' - Extract single scalar value
;; - `duckdb-query-column' - Extract single column as list
;; - `duckdb-query-describe' - Schema introspection
;; - `duckdb-query-with-database' - Scoped database context
;; - `duckdb-query-with-transient-database' - Temporary file-based database
;;
;; Output formats via :format parameter:
;; - :alist (default), :plist, :hash, :vector, :columnar, :org-table
;;
;; Execution strategies via :executor parameter:
;; - :cli (default) - Direct CLI invocation
;; - function - Custom executor function
;; - Custom objects via `cl-defmethod'
;;
;; For benchmarking, see duckdb-query-bench.el.
;;; Code:

(require 'cl-lib)
(require 'ansi-color)
(require 'ob-ref)

;;;; Customization

(defgroup duckdb-query nil
  "Execute DuckDB queries and parse results into elisp data structures."
  :group 'data
  :prefix "duckdb-query-")

(defcustom duckdb-query-executable "duckdb"
  "Path to DuckDB executable.

Used by `duckdb-query-execute-raw' and the `:cli' executor method."
  :type 'string
  :group 'duckdb-query
  :package-version '(duckdb-query . "0.1.0"))

(defcustom duckdb-query-default-timeout 30
  "Default timeout in seconds for query execution.

Used by `:cli' executor when :timeout argument is nil."
  :type 'integer
  :group 'duckdb-query
  :package-version '(duckdb-query . "0.1.0"))

(defcustom duckdb-query-null-value :null
  "Value representing SQL NULL in query results.

Used when parsing JSON output from DuckDB."
  :type '(choice (const :tag "Keyword :null" :null)
                 (const :tag "Symbol nil" nil)
                 (const :tag "String \"NULL\"" "NULL"))
  :group 'duckdb-query
  :package-version '(duckdb-query . "0.1.0"))

(defcustom duckdb-query-false-value :false
  "Value representing SQL FALSE in query results.

Used when parsing JSON output from DuckDB."
  :type '(choice (const :tag "Keyword :false" :false)
                 (const :tag "Symbol nil" nil))
  :group 'duckdb-query
  :package-version '(duckdb-query . "0.1.0"))

;;;; Internal Variables

(defvar duckdb-query-default-database nil
  "Default database file for `duckdb-query' when not specified.

When non-nil, all queries use this database unless overridden by
explicit :database parameter.

Set via `duckdb-query-set-default-database' or dynamically bound
via `duckdb-query-with-database'.

Nil means in-memory transient database.")

(defvar duckdb-query--transient-database-p nil
  "Non-nil when inside `duckdb-query-with-transient-database' scope.

Used by CLI executor to default to writable mode, enabling database
file creation.  Without this, the CLI executor defaults to read-only
mode when a database path is specified, which fails for non-existent
databases.")

;;;; Data Extraction Utilities

(defun duckdb-query-value (query &rest args)
  "Return single scalar value from QUERY.

QUERY is SQL string that must return exactly one row with one column.
ARGS are passed to `duckdb-query'.

Returns the scalar value, or nil for empty results.

Signals `user-error' if result has multiple rows or columns.

Example:

  (duckdb-query-value \"SELECT COUNT(*) FROM users\")
  ;; => 42

  (duckdb-query-value \"SELECT MAX(price) FROM orders\"
                      :database \"sales.db\")
  ;; => 99.95

Also see `duckdb-query-column' for extracting entire columns."
  (let* ((result (apply #'duckdb-query query :format :alist args))
         (row (car result)))
    (cond
     ((null result)
      nil)
     ((cdr result)
      (user-error "Query returned %d rows, expected 1"
                  (length result)))
     ((cdr row)
      (user-error "Query returned %d columns, expected 1"
                  (length row)))
     (t
      (cdar row)))))

(defun duckdb-query-column (query &rest args)
  "Return single column from QUERY as list.

QUERY is SQL string to execute.

ARGS are keyword arguments:
  :column    - Column name string.  When nil, return first column.
  :as-vector - When non-nil, return vector instead of list.

Other ARGS are passed to `duckdb-query'.

Return list of column values, or vector if :as-vector is non-nil.
Return nil for empty results.

Example:

  (duckdb-query-column \"SELECT name FROM users ORDER BY id\")
  ;; => (\"Alice\" \"Bob\" \"Carol\")

  (duckdb-query-column \"SELECT id, name FROM users\"
                       :column \"name\")
  ;; => (\"Alice\" \"Bob\" \"Carol\")

  (duckdb-query-column \"SELECT value FROM data\" :as-vector t)
  ;; => [1 2 3 4 5]

Also see `duckdb-query-value' for scalar extraction.
Also see `duckdb-query' with :format :columnar for multiple columns."
  (let* ((column (plist-get args :column))
         (as-vector (plist-get args :as-vector))
         (clean-args (cl-loop for (k v) on args by #'cddr
                              unless (memq k '(:column :as-vector))
                              append (list k v)))
         (result (apply #'duckdb-query query :format :columnar clean-args))
         (col-data (if column
                       ;; Column names in columnar format are symbols
                       (cdr (assq (intern column) result))
                     (cdar result))))
    (cond
     ((null col-data)
      nil)
     (as-vector
      col-data)
     (t
      (append col-data nil)))))

;;;; Schema Introspection

(defun duckdb-query-describe (source &rest args)
  "Return schema information for SOURCE.

SOURCE can be:
- Table name: \"users\"
- File path: \"/path/to/data.parquet\" or \"~/data.csv\"
- Remote URL: \"https://example.com/data.csv\"
- Cloud storage: \"s3://bucket/data.parquet\"
- SELECT query: \"SELECT * FROM users WHERE active\"
- CTE query: \"WITH cte AS (...) SELECT ...\"

ARGS are passed to `duckdb-query'.

For SELECT and WITH queries, wrap SOURCE in DESCRIBE(...).
For tables, files, and URLs, use DESCRIBE directly.

Return list of alists, one per column:

  (((column_name . \"id\")
    (column_type . \"INTEGER\")
    (null . \"YES\")
    (key . :null)
    (default . :null)
    (extra . :null))
   ...)

The `column_name' and `column_type' fields are always populated.
Other fields may be :null depending on SOURCE type; database tables
with constraints populate `key' and `default'.

Signal `user-error' for DDL statements (CREATE, INSERT, DROP, etc.)
which cannot be described directly.  Create the table first, then
describe it by name.

Use `duckdb-query-default-database' when SOURCE is a table name.

Example:

  (duckdb-query-describe \"users\")
  (duckdb-query-describe \"~/data/sales.parquet\")
  (duckdb-query-describe \"SELECT a, b FROM t WHERE x > 10\")

Also see `duckdb-query-columns' to extract column names.
Also see `duckdb-query-column-types' for name-to-type mapping."
  (let* ((source-trimmed (string-trim source))
         (source-upper (upcase source-trimmed))
         (query
          (cond
           ;; File path (absolute, home-relative, or with data file extension)
           ((or (string-prefix-p "/" source-trimmed)
                (string-prefix-p "~" source-trimmed)
                (string-match-p "\\.\\(parquet\\|csv\\|json\\|jsonl\\|ndjson\\)\\(?:\\.gz\\)?\\'"
                                source-trimmed))
            (format "DESCRIBE '%s'" source-trimmed))
           ;; Remote URL (http, https, s3, gs, az, hf)
           ((string-match-p "\\`\\(https?\\|s3\\|gs\\|az\\|hf\\)://" source-trimmed)
            (format "DESCRIBE '%s'" source-trimmed))
           ;; SELECT or WITH query - wrap in DESCRIBE(...)
           ((string-match-p "\\`\\(SELECT\\|WITH\\)\\b" source-upper)
            (format "DESCRIBE (%s)" source-trimmed))
           ;; DDL - reject with guidance
           ((string-match-p
             "\\`\\(CREATE\\|INSERT\\|UPDATE\\|DELETE\\|DROP\\|ALTER\\|TRUNCATE\\)\\b"
             source-upper)
            (user-error
             "Cannot describe DDL statement; create table first, then describe by name"))
           ;; Assume table name
           (t
            (format "DESCRIBE %s" source-trimmed)))))
    (apply #'duckdb-query query :format :alist args)))

(defun duckdb-query-columns (source &rest args)
  "Return list of column names for SOURCE.

SOURCE can be table name, file path, URL, or SELECT query.
ARGS are passed to `duckdb-query-describe'.

Return list of column name strings.

Example:

  (duckdb-query-columns \"users\")
  ;; => (\"id\" \"name\" \"email\" \"created_at\")

  (duckdb-query-columns \"~/data/sales.parquet\")
  ;; => (\"date\" \"product\" \"quantity\" \"price\")

Also see `duckdb-query-describe' for full column metadata.
Also see `duckdb-query-column-types' for name-to-type mapping."
  (mapcar (lambda (row) (cdr (assq 'column_name row)))
          (apply #'duckdb-query-describe source args)))

(defun duckdb-query-column-types (source &rest args)
  "Return alist mapping column names to DuckDB types for SOURCE.

SOURCE can be table name, file path, URL, or SELECT query.
ARGS are passed to `duckdb-query-describe'.

Return alist of (NAME . TYPE) pairs where NAME is column name
string and TYPE is DuckDB type string.

Example:

  (duckdb-query-column-types \"users\")
  ;; => ((\"id\" . \"INTEGER\")
  ;;     (\"name\" . \"VARCHAR\")
  ;;     (\"created_at\" . \"TIMESTAMP\"))

Also see `duckdb-query-describe' for full column metadata.
Also see `duckdb-query-columns' for just column names."
  (mapcar (lambda (row)
            (cons (cdr (assq 'column_name row))
                  (cdr (assq 'column_type row))))
          (apply #'duckdb-query-describe source args)))

;;;; Context Management Macro

(defmacro duckdb-query-with-database (database &rest body)
  "Execute BODY with DATABASE as context.

DATABASE is file path string, evaluated once.

Behavior depends on execution context:

Within session scope (inside `duckdb-query-with-session'):
  ATTACHes DATABASE as read-only with alias derived from filename.
  Executes BODY with database available via alias.
  DETACHes database after BODY completes, even on error.
  Tables accessed as ALIAS.table_name.

Outside session scope:
  Binds `duckdb-query-default-database' to DATABASE for BODY.
  All `duckdb-query' calls use DATABASE unless overridden by
  explicit :database parameter.

Return value of last form in BODY.

Example outside session:

  (duckdb-query-with-database \"app.db\"
    (duckdb-query \"CREATE TABLE users (id INTEGER, name TEXT)\"
                  :readonly nil)
    (duckdb-query \"INSERT INTO users VALUES (1, \\='Alice\\=')\"
                  :readonly nil)
    (duckdb-query \"SELECT * FROM users\"))
  ;; => (((id . 1) (name . \"Alice\")))

Example within session:

  (duckdb-query-with-session \"work\"
    (duckdb-query-with-database \"/path/to/sales.db\"
      ;; sales.db attached as \\='sales\\=' (filename without extension)
      (duckdb-query \"SELECT * FROM sales.orders\")))
  ;; Database automatically detached after body

Also see `duckdb-query-with-session' for session-scoped execution.
Also see `duckdb-query-session-attach' for manual attachment."
  (declare (indent 1) (debug t))
  (let ((db-sym (make-symbol "database"))
        (alias-sym (make-symbol "alias")))
    `(let ((,db-sym ,database))
       (if duckdb-query--current-session
           (let ((,alias-sym (duckdb-query-session-attach
                              duckdb-query--current-session
                              ,db-sym)))
             (unwind-protect
                 (progn ,@body)
               (ignore-errors
                 (duckdb-query-session-detach
                  duckdb-query--current-session
                  ,alias-sym))))
         (let ((duckdb-query-default-database ,db-sym))
           ,@body)))))

(defmacro duckdb-query-with-transient-database (&rest body)
  "Execute BODY with temporary file-based database.

Create temporary database file, execute BODY with that database as
context, then delete the file.

Behavior depends on execution context:

Outside session scope:
  Bind `duckdb-query-default-database' to temp file.
  All `duckdb-query' calls use this database via CLI executor.
  Each query spawns new DuckDB process but shares database state.
  Queries default to writable mode to enable database creation.

Within session scope (inside `duckdb-query-with-session'):
  ATTACH temp database to session with auto-generated alias.
  Execute BODY with database available via alias.
  DETACH and delete temp file after BODY completes.
  Access tables as _temp_N.table_name (alias shown in return).

Return value of last form in BODY.

The temporary database file is deleted even if BODY signals error.

Example outside session:

  (duckdb-query-with-transient-database
    (duckdb-query \"CREATE TABLE temp (id INTEGER)\")
    (duckdb-query \"INSERT INTO temp VALUES (1), (2), (3)\")
    (duckdb-query \"SELECT COUNT(*) as count FROM temp\"))
  ;; => (((count . 3)))

Example within session:

  (duckdb-query-with-session \"work\"
    (duckdb-query-with-transient-database
      ;; Temp database attached, tables accessible
      (duckdb-query \"CREATE TABLE scratch AS SELECT 1 as val\")
      (duckdb-query \"SELECT * FROM scratch\")))
  ;; Temp database automatically detached and deleted

Note: For session-scoped temporary state that doesn't need file
persistence, consider using the session's built-in temp database
directly (tables created in session persist until session killed).

Also see `duckdb-query-with-database' for named database files.
Also see `duckdb-query-with-transient-session' for ephemeral sessions."
  (declare (indent 0) (debug t))
  (let ((db-var (make-symbol "transient-db"))
        (alias-var (make-symbol "transient-alias")))
    `(let ((,db-var (expand-file-name
                     (concat (make-temp-name "duckdb-transient-")
                             ".duckdb")
                     temporary-file-directory)))
       (if duckdb-query--current-session
           ;; Session context: ATTACH/DETACH
           (let ((,alias-var (duckdb-query-session-attach
                              duckdb-query--current-session
                              ,db-var
                              nil    ; auto-generate alias
                              nil))) ; writable
             (unwind-protect
                 (progn ,@body)
               (ignore-errors
                 (duckdb-query-session-detach
                  duckdb-query--current-session
                  ,alias-var))
               (when (file-exists-p ,db-var)
                 (delete-file ,db-var))))
         ;; CLI context: bind default database with transient flag
         (unwind-protect
             (let ((duckdb-query-default-database ,db-var)
                   (duckdb-query--transient-database-p t))
               ,@body)
           (when (file-exists-p ,db-var)
             (delete-file ,db-var)))))))

;;;; Interactive Functions

;;;###autoload
(defun duckdb-query-set-default-database (&optional path)
  "Set default database for `duckdb-query' to PATH.

If PATH is nil or empty string, use in-memory database.

Return previous default database.

When called interactively, prompt for database file path.

Example:

  ;; Set global default
  (duckdb-query-set-default-database \"app.db\")

  ;; All subsequent queries use app.db
  (duckdb-query \"SELECT * FROM users\")

  ;; Clear default (use in-memory)
  (duckdb-query-set-default-database nil)"
  (interactive "fDatabase file (blank for in-memory): ")
  (prog1 duckdb-query-default-database
    (setq duckdb-query-default-database
          (if (and path (not (string-empty-p path)))
              (expand-file-name path)
            nil))))

(define-error 'duckdb-query-copy-failed
  "DuckDB COPY strategy failed, fallback to pipe required"
  'error)

;;;; Executor Protocol
;;;;; Generic Function

(cl-defgeneric duckdb-query-execute (executor _query &rest _args)
  "Execute QUERY using EXECUTOR strategy, return JSON string.

This is the core extension point for pluggable execution strategies.
Implement methods for custom executors to integrate different backends
while leveraging the unified conversion layer.

EXECUTOR controls execution strategy:
  :cli       - Direct CLI invocation (default)
  function   - Custom executor function
  symbol     - Function name as symbol
  object     - Custom executor object with `cl-defmethod'

QUERY is SQL string to execute.

ARGS are executor-specific parameters passed as plist.  The `:cli'
executor supports:
  :database - Database file path (nil for in-memory)
  :readonly - Open database read-only
  :timeout  - Execution timeout in seconds

Return JSON string for parsing by conversion layer.
Signal error on execution failure.

To implement custom executor, define method with `cl-defmethod':

  (cl-defmethod duckdb-query-execute ((exec my-type) query &rest args)
    \"Execute QUERY with custom backend.\"
    ...)

Called by `duckdb-query' to delegate execution.
Also see Info node `(duckdb-query) Executors' for details."
  (error "No executor method defined for %S" executor))

;;;;; CLI Executor

(defun duckdb-query--build-cli-args (&rest params)
  "Build DuckDB CLI argument list from PARAMS.

PARAMS is plist with keys:
  :database     - Database file path (nil for in-memory)
  :readonly     - Open database read-only
  :output-mode  - DuckDB output mode (json, csv, etc.)
  :init-file    - SQL file to execute before query
  :separator    - Column separator for CSV mode
  :nullvalue    - String to display for NULL values

Return list of strings suitable for `call-process' invocation.

Used by `duckdb-query-execute' `:cli' method to construct command-line
arguments from keyword parameters.

Example:
  (duckdb-query--build-cli-args
   :database \"test.db\"
   :readonly t
   :output-mode \\='json)
  ;; => (\"duckdb\" \"test.db\" \"-readonly\" \"-json\")"
  (let ((args (list duckdb-query-executable))
        (database (plist-get params :database))
        (readonly (plist-get params :readonly))
        (output-mode (plist-get params :output-mode))
        (init-file (plist-get params :init-file))
        (separator (plist-get params :separator))
        (nullvalue (plist-get params :nullvalue)))

    ;; param insertion
    (when database (push (expand-file-name database) args))
    (when readonly (push "-readonly" args))
    (when output-mode (push (format "-%s" output-mode) args))
    (when init-file (push "-init" args) (push (expand-file-name init-file) args))
    (when separator (push "-separator" args) (push separator args))
    (when nullvalue (push "-nullvalue" args) (push nullvalue args))

    (nreverse args)))

(defun duckdb-query--invoke-cli (cli-args query _timeout)
  "Invoke DuckDB CLI with CLI-ARGS, executing QUERY with TIMEOUT.

CLI-ARGS is list of command-line arguments from
`duckdb-query--build-cli-args'.
QUERY is SQL string to execute.
TIMEOUT is execution timeout in seconds (currently unused).

Return output string on success.
Signal error with DuckDB's message on failure.

Uses temp file with `-f' flag for sequential statement execution,
enabling extension syntax that requires LOAD before use.

Called by `duckdb-query-execute' `:cli' method."
  (let ((sql-file (make-temp-file "duckdb-pipe-" nil ".sql")))
    (unwind-protect
        (progn
          (with-temp-file sql-file
            (insert query))
          (with-temp-buffer
            (let* ((default-directory temporary-file-directory)
                   (process-environment (cons "DUCKDB_NO_COLOR=1" process-environment))
                   (exit-code (apply #'call-process
                                     (car cli-args) nil t nil
                                     (append (cdr cli-args)
                                             (list "-f" sql-file))))
                   (output (buffer-string)))
              (if (zerop exit-code)
                  output
                (error "DuckDB execution failed (exit %d): %s"
                       exit-code (string-trim output))))))
      (when (file-exists-p sql-file)
        (delete-file sql-file)))))

(defun duckdb-query--invoke-cli-file (cli-args query _timeout)
  "Execute QUERY via COPY TO with file-based I/O for better performance.

CLI-ARGS is list of command-line arguments from `duckdb-query--build-cli-args'.
QUERY is SQL string to execute.
TIMEOUT is execution timeout in seconds (currently unused).

Wraps QUERY in COPY statement writing to temporary JSON file.
Uses `-bail' flag to stop on first error and `-f' to execute from file.

Returns JSON string on success.
Signals `duckdb-query-copy-failed' on failure with error output for fallback
handling.

The COPY wrapper places QUERY on separate line so parser errors
reference the user's SQL without showing COPY scaffolding.

Called by `duckdb-query-execute' `:cli' method when file output is viable."
  (let ((sql-file (make-temp-file "duckdb-query-" nil ".sql"))
        (json-file (make-temp-file "duckdb-output-" nil ".json")))
    (unwind-protect
        (progn
          ;; Write SQL file with COPY wrapper
          ;; Query on separate line hides COPY from error messages
          (with-temp-file sql-file
            (insert "COPY (\n")
            (insert query)
            (insert "\n) TO '" json-file "' (FORMAT json, ARRAY true);\n"))

          ;; Execute with -bail to stop on first error
          (with-temp-buffer
            (let* ((default-directory temporary-file-directory)
                   (process-environment (cons "DUCKDB_NO_COLOR=1" process-environment))
                   (exit-code (apply #'call-process
                                     (car cli-args) nil t nil
                                     (append (cdr cli-args)
                                             (list "-bail" "-f" sql-file))))
                   (error-output (buffer-string)))
              (if (and (zerop exit-code)
                       (file-exists-p json-file)
                       (> (file-attribute-size (file-attributes json-file)) 0))
                  ;; Success: read JSON output
                  (with-temp-buffer
                    (insert-file-contents json-file)
                    (buffer-string))
                ;; Failure: signal for fallback to pipe mode
                (signal 'duckdb-query-copy-failed (list error-output))))))
      ;; Cleanup temp files
      (when (file-exists-p sql-file) (delete-file sql-file))
      (when (file-exists-p json-file) (delete-file json-file)))))

(cl-defmethod duckdb-query-execute ((_executor (eql :cli)) query &rest args)
  "Execute QUERY via DuckDB CLI with ARGS parameters.

Supported ARGS:
  :database     - Database file path (nil for in-memory)
  :readonly     - Open database read-only (default t when :database provided,
                  except nil when inside `duckdb-query-with-transient-database')
  :timeout      - Execution timeout in seconds
  :output-via   - Output strategy: `:file' (default) or `:pipe'
  :output-mode  - DuckDB output mode (default \\='json for pipe mode)
  :init-file    - SQL file to execute before query
  :separator    - Column separator for CSV mode
  :nullvalue    - String to display for NULL values

The `:file' strategy wraps QUERY in COPY statement for ~8x faster
output on large result sets.  Nested types (STRUCT, LIST, MAP, ARRAY)
serialize correctly as JSON without requiring `:preserve-nested'.

If `:file' strategy fails (e.g., for DDL statements), automatically
falls back to `:pipe' strategy.

The `:pipe' strategy uses traditional `-json' CLI output.  Required
for DDL, DML, DESCRIBE, and other non-SELECT statements.

Return JSON string from DuckDB output.
Signal error on execution failure after fallback."
  (let* ((database (plist-get args :database))
         (readonly (if (plist-member args :readonly)
                       (plist-get args :readonly)
                     ;; Default: writable for transient databases, readonly for others
                     (and database (not duckdb-query--transient-database-p))))
         (timeout (or (plist-get args :timeout)
                      duckdb-query-default-timeout))
         (output-via (or (plist-get args :output-via) :file))
         (output-mode (or (plist-get args :output-mode) 'json))
         (init-file (plist-get args :init-file))
         (separator (plist-get args :separator))
         (nullvalue (plist-get args :nullvalue))
         (cli-args (duckdb-query--build-cli-args
                    :database database
                    :readonly readonly
                    :init-file init-file
                    :separator separator
                    :nullvalue nullvalue)))
    (if (eq output-via :pipe)
        ;; Explicit pipe mode: use traditional JSON output
        (let ((pipe-args (duckdb-query--build-cli-args
                          :database database
                          :readonly readonly
                          :output-mode output-mode
                          :init-file init-file
                          :separator separator
                          :nullvalue nullvalue)))
          (duckdb-query--invoke-cli pipe-args query timeout))
      ;; File mode (default): try COPY, fallback to pipe on failure
      (condition-case nil
          (duckdb-query--invoke-cli-file cli-args query timeout)
        (duckdb-query-copy-failed
         ;; Fallback to pipe mode for DDL/DML/DESCRIBE
         (let ((pipe-args (duckdb-query--build-cli-args
                           :database database
                           :readonly readonly
                           :output-mode output-mode
                           :init-file init-file
                           :separator separator
                           :nullvalue nullvalue)))
           (duckdb-query--invoke-cli pipe-args query timeout)))))))

;;;;; Session Executor
;;;;;; Customization

(defgroup duckdb-query-session nil
  "Session-based execution for low-latency DuckDB queries."
  :group 'duckdb-query
  :prefix "duckdb-query-session-")

(defcustom duckdb-query-session-init-commands nil
  "SQL commands to execute when session process starts.

List of SQL strings run during process initialization.  Useful for
loading extensions or setting session-wide configuration.

Example:
  (setq duckdb-query-session-init-commands
        \\='(\"INSTALL spatial\" \"LOAD spatial\"))"
  :type '(repeat string)
  :group 'duckdb-query-session
  :package-version '(duckdb-query . "0.7.0"))

;;;;;; Hooks

(defvar duckdb-query-session-created-functions nil
  "Hook run when session is created.

Functions receive (NAME SESSION):
  NAME    - Session name string
  SESSION - `duckdb-session' struct

Called after initialization completes successfully.")

(defvar duckdb-query-session-killed-functions nil
  "Hook run when session is killed.

Functions receive (NAME):
  NAME - Session name string

Called after process termination and cleanup.")

(defvar duckdb-query-session-query-executed-functions nil
  "Hook run after query execution in session.

Functions receive (NAME QUERY DURATION-MS):
  NAME        - Session name string
  QUERY       - SQL string executed
  DURATION-MS - Execution time in milliseconds

Called after successful query completion.")

;;;;;; Data Structures

(cl-defstruct (duckdb-session
               (:constructor duckdb-session--create)
               (:copier nil))
  "Session representing a persistent DuckDB process.

Sessions maintain state across queries: temp tables, loaded extensions,
and settings persist until session is killed."
  (name nil :documentation "Session name string.")
  (temp-db nil :documentation "Temp database file path.")
  (process nil :documentation "Emacs process object.")
  (output "" :documentation "Accumulated process output.")
  (status 'active :documentation "Status: active, error, initializing.")
  (owners nil :documentation "List of buffers owning this session.")
  (created-at nil :documentation "Creation timestamp.")
  (last-used nil :documentation "Most recent query timestamp.")
  (query-count 0 :documentation "Total queries executed."))

(defvar duckdb-query-sessions (make-hash-table :test 'equal)
  "Registry mapping session names to `duckdb-session' structs.")

;;;;;; Internal Helpers
(defun duckdb-query--strip-ansi (string)
  "Remove ANSI escape sequences from STRING.
Uses `ansi-color-filter-apply' to strip SGR control sequences
that DuckDB CLI outputs for colored error messages."
  (when string
    (ansi-color-filter-apply string)))

(defun duckdb-query-session--make-filter (session)
  "Create process filter accumulating output in SESSION."
  (lambda (_proc string)
    (setf (duckdb-session-output session)
          (concat (duckdb-session-output session) string))))

(defun duckdb-query-session--initialize (proc session)
  "Initialize SESSION process PROC with startup commands."
  (condition-case err
      (progn
        (accept-process-output proc 2.0)
        (setf (duckdb-session-output session) "")
        (dolist (cmd duckdb-query-session-init-commands)
          (process-send-string proc (concat cmd ";\n"))
          (accept-process-output proc 1.0))
        (setf (duckdb-session-output session) "")
        (setf (duckdb-session-status session) 'active))
    (error
     (setf (duckdb-session-status session) 'error)
     (signal (car err) (cdr err)))))

(defun duckdb-query-session--extract-json (output)
  "Extract JSON from OUTPUT, stripping trailing prompt.

DuckDB outputs: prompt + JSON + newline + prompt.
Example: \"D [{...}]\\nD\"

Returns JSON string, or trimmed non-JSON content."
  (let ((trimmed (string-trim output)))
    (cond
     ;; Empty
     ((string-empty-p trimmed) "")

     ;; Starts with D followed by space/bracket - strip leading prompt
     ((and (> (length trimmed) 2)
           (eq (aref trimmed 0) ?D)
           (memq (aref trimmed 1) '(?\s ?\[)))
      ;; Recurse with prompt stripped
      (duckdb-query-session--extract-json (substring trimmed 1)))

     ;; JSON array - find last ]
     ((eq (aref trimmed 0) ?\[)
      (let ((last-pos (cl-position ?\] trimmed :from-end t)))
        (if last-pos
            (substring trimmed 0 (1+ last-pos))
          trimmed)))

     ;; JSON object - find last }
     ((eq (aref trimmed 0) ?\{)
      (let ((last-pos (cl-position ?\} trimmed :from-end t)))
        (if last-pos
            (substring trimmed 0 (1+ last-pos))
          trimmed)))

     ;; Not JSON
     (t trimmed))))

;;;;;; Session Lifecycle

(defun duckdb-query-session-start (name)
  "Start session NAME with fresh temp database.

NAME is string identifying the session.

Returns `duckdb-session' struct.
Fires `duckdb-query-session-created-functions' hook.
Signals error if session NAME already exists."
  (when (gethash name duckdb-query-sessions)
    (error "Session %s already exists" name))
  (let* ((temp-db (concat (make-temp-name
                           (expand-file-name "duckdb-session-"
                                             temporary-file-directory))
                          ".duckdb"))
         (proc (start-process (format "duckdb-session-%s" name)
                              nil
                              duckdb-query-executable
                              temp-db
                              "-json"))
         (session (duckdb-session--create
                   :name name
                   :temp-db temp-db
                   :process proc
                   :output ""
                   :status 'initializing
                   :created-at (current-time)
                   :last-used (current-time)
                   :query-count 0)))
    (set-process-filter proc (duckdb-query-session--make-filter session))
    (puthash name session duckdb-query-sessions)
    (duckdb-query-session--initialize proc session)
    ;; Fire hook
    (run-hook-with-args 'duckdb-query-session-created-functions name session)
    session))

(defun duckdb-query-session-get (name)
  "Get session NAME, or nil if not exists."
  (gethash name duckdb-query-sessions))

(defun duckdb-query-session-kill (name)
  "Kill session NAME and delete its temp database.

Fires `duckdb-query-session-killed-functions' hook."
  (when-let ((session (gethash name duckdb-query-sessions)))
    (let ((proc (duckdb-session-process session))
          (temp-db (duckdb-session-temp-db session)))
      (when (process-live-p proc)
        (delete-process proc))
      (when (and temp-db (file-exists-p temp-db))
        (delete-file temp-db)))
    (remhash name duckdb-query-sessions)
    ;; Fire hook
    (run-hook-with-args 'duckdb-query-session-killed-functions name)))

(defun duckdb-query-session-list ()
  "Return list of all session structs."
  (let ((result nil))
    (maphash (lambda (_k v) (push v result))
             duckdb-query-sessions)
    (nreverse result)))

;;;;;; Error Condition for Fallback

(define-error 'duckdb-query-session-copy-failed
              "Session COPY strategy failed, fallback to pipe"
              'error)

;;;;;; File-Based Execution

(defun duckdb-query-session--execute-via-file (session query timeout)
  "Execute QUERY in SESSION using COPY TO temp file with .read for input.

Writes COPY-wrapped query to temp SQL file, sends .read command to
session to execute it, waits for completion marker.

Returns JSON string from output file.
Signals `duckdb-query-session-copy-failed' if COPY fails."
  (let* ((proc (duckdb-session-process session))
         (sql-file (make-temp-file "duckdb-session-query-" nil ".sql"))
         (json-file (make-temp-file "duckdb-session-result-" nil ".json"))
         (marker (format "DUCKDB_FILE_COMPLETE_%s"
                         (md5 (format "%s%s" (duckdb-session-name session)
                                      (current-time))))))
    (unwind-protect
        (progn
          ;; Write COPY-wrapped query to SQL file
          (with-temp-file sql-file
            (insert (format "COPY (\n%s\n) TO '%s' (FORMAT json, ARRAY true);\n"
                            query json-file)))

          (setf (duckdb-session-output session) "")

          ;; Send .read command instead of raw query
          (process-send-string
           proc
           (format ".read '%s'\n.print \"%s\"\n" sql-file marker))

          ;; Wait for marker
          (let ((start (current-time))
                (found nil))
            (while (and (not found)
                        (< (float-time (time-since start)) timeout))
              (accept-process-output proc 0.001)
              (when (string-search marker (duckdb-session-output session))
                (setq found t)))
            (unless found
              (setf (duckdb-session-status session) 'error)
              (error "Query timed out after %d seconds" timeout))
            ;; Check for errors after marker found
            (let ((clean-output (duckdb-query--strip-ansi
                                 (duckdb-session-output session))))
              (when (string-match-p
                     "\\(?:Error\\|Exception\\|SYNTAX_ERROR\\|CATALOG_ERROR\\|BINDER_ERROR\\|PARSER_ERROR\\):"
                     clean-output)
                (signal 'duckdb-query-session-copy-failed (list clean-output))))
            ;; Read result from JSON file
            (if (and (file-exists-p json-file)
                     (> (file-attribute-size (file-attributes json-file)) 0))
                (with-temp-buffer
                  (insert-file-contents json-file)
                  (buffer-string))
              "[]")))
      ;; Cleanup temp files
      (when (file-exists-p sql-file) (delete-file sql-file))
      (when (file-exists-p json-file) (delete-file json-file)))))

;;;;;; Pipe-Based Execution (for DDL/DML fallback)

(defun duckdb-query-session--execute-via-pipe (session query timeout)
  "Execute QUERY in SESSION using .read for input with pipe-based output.

Writes query to temp SQL file, sends .read command to session.
Used for DDL/DML statements that cannot use COPY.

Returns output string (may be empty for DDL).
Uses `duckdb-query-session--extract-json' to strip trailing prompt."
  (let* ((proc (duckdb-session-process session))
         (sql-file (make-temp-file "duckdb-session-query-" nil ".sql"))
         (marker (format "DUCKDB_PIPE_COMPLETE_%s"
                         (md5 (format "%s%s" (duckdb-session-name session)
                                      (current-time)))))
         (start-time (current-time)))
    (unwind-protect
        (progn
          ;; Write query to SQL file
          (with-temp-file sql-file
            (insert query)
            (insert ";\n"))

          (setf (duckdb-session-output session) "")

          ;; Send .read command instead of raw query
          (process-send-string proc (format ".read '%s'\n.print \"%s\"\n"
                                            sql-file marker))

          (let ((found nil))
            (while (and (not found)
                        (< (float-time (time-since start-time)) timeout))
              (accept-process-output proc 0.001)
              (when (string-search marker (duckdb-session-output session))
                (setq found t)))
            (unless found
              (setf (duckdb-session-status session) 'error)
              (error "Query timed out after %d seconds" timeout)))

          ;; Extract and clean result - strip ANSI codes
          (duckdb-query--strip-ansi
           (duckdb-query-session--extract-json
            (substring (duckdb-session-output session)
                       0
                       (string-search marker (duckdb-session-output session))))))
      ;; Cleanup temp file
      (when (file-exists-p sql-file) (delete-file sql-file)))))

;;;;;; Unified Execution with Fallback

(defun duckdb-query-session-execute (name query timeout)
  "Execute QUERY in session NAME, wait up to TIMEOUT seconds.

Uses file-based output for performance.  Falls back to pipe for
DDL/DML statements that cannot use COPY.

Fires `duckdb-query-session-query-executed-functions' hook.
Returns result string."
  (let* ((session (or (gethash name duckdb-query-sessions)
                      (error "Session %s does not exist" name)))
         (proc (duckdb-session-process session))
         (start-time (current-time))
         (result nil))
    (unless (process-live-p proc)
      (error "Session %s process is dead" name))
    ;; Try file-based, fallback to pipe
    (setq result
          (condition-case _err
              (duckdb-query-session--execute-via-file session query timeout)
            (duckdb-query-session-copy-failed
             (duckdb-query-session--execute-via-pipe session query timeout))))
    ;; Update stats and fire hook
    (let ((duration-ms (* 1000 (float-time (time-since start-time)))))
      (setf (duckdb-session-last-used session) (current-time))
      (cl-incf (duckdb-session-query-count session))
      (run-hook-with-args 'duckdb-query-session-query-executed-functions
                          name query duration-ms))
    result))
;;;;;; Scoped Session Variables

(defvar-local duckdb-query--current-session nil
  "Current session name for scoped execution.
Bound by `duckdb-query-with-session'.")

;;;;;; Scoped Execution Macros

(defmacro duckdb-query-with-session (name &rest body)
  "Execute BODY with queries routed to session NAME.

Session must exist (created via `duckdb-query-session-start').
Session persists after BODY completes.

Within BODY, `duckdb-query' calls without explicit :executor
automatically use the session.

Example:
  (duckdb-query-session-start \"work\")
  (duckdb-query-with-session \"work\"
    (duckdb-query \"CREATE TABLE t AS SELECT 1\")
    (duckdb-query \"SELECT * FROM t\"))
  (duckdb-query-session-kill \"work\")"
  (declare (indent 1) (debug t))
  (let ((name-sym (make-symbol "session-name")))
    `(let* ((,name-sym ,name)
            (duckdb-query--current-session ,name-sym))
       (unless (duckdb-query-session-get ,name-sym)
         (error "Session %s does not exist" ,name-sym))
       ,@body)))

(defmacro duckdb-query-with-transient-session (&rest body)
  "Execute BODY with temporary session, killed after completion.

Creates anonymous session, executes BODY, kills session.
Temp database is deleted automatically.

Example:
  (duckdb-query-with-transient-session
    (duckdb-query \"CREATE TABLE t AS SELECT 1\")
    (duckdb-query \"SELECT * FROM t\"))"
  (declare (indent 0) (debug t))
  (let ((name-sym (make-symbol "transient-name")))
    `(let ((,name-sym (format "transient-%s" (md5 (format "%s" (current-time))))))
       (duckdb-query-session-start ,name-sym)
       (unwind-protect
           (let ((duckdb-query--current-session ,name-sym))
             ,@body)
         (duckdb-query-session-kill ,name-sym)))))

;;;;;; Executor Auto-Resolution

(defun duckdb-query--resolve-executor (explicit-executor)
  "Resolve executor from EXPLICIT-EXECUTOR or session context.

Returns :session if inside `duckdb-query-with-session' scope,
otherwise returns EXPLICIT-EXECUTOR or :cli."
  (cond
   (explicit-executor explicit-executor)
   (duckdb-query--current-session :session)
   (t :cli)))

;;;;;; Executor Integration

(cl-defmethod duckdb-query-execute ((_executor (eql :session)) query &rest args)
  "Execute QUERY via named session.

ARGS supports:
  :name    - Session name (uses `duckdb-query--current-session' if nil)
  :timeout - Query timeout seconds

Session must exist or be in scope via `duckdb-query-with-session'."
  (let ((session-name (or (plist-get args :name)
                          duckdb-query--current-session
                          (error "No session specified and not in session scope")))
        (timeout (or (plist-get args :timeout)
                     duckdb-query-default-timeout)))
    (duckdb-query-session-execute session-name query timeout)))
;;;;;; Ownership Management

(defun duckdb-query-session-register-owner (name buffer)
  "Register BUFFER as owner of session NAME.

Session won't be auto-killed while it has owners.
Owner is automatically unregistered when buffer is killed.

Returns session struct."
  (let ((session (or (gethash name duckdb-query-sessions)
                     (error "Session %s does not exist" name))))
    (unless (memq buffer (duckdb-session-owners session))
      (push buffer (duckdb-session-owners session))
      (with-current-buffer buffer
        (add-hook 'kill-buffer-hook
                  (lambda ()
                    (duckdb-query-session-unregister-owner name (current-buffer)))
                  nil t)))
    session))

(defun duckdb-query-session-unregister-owner (name buffer)
  "Unregister BUFFER as owner of session NAME."
  (when-let ((session (gethash name duckdb-query-sessions)))
    (setf (duckdb-session-owners session)
          (delq buffer (duckdb-session-owners session)))))
;;;;;; Interactive Commands

;;;###autoload
(defun duckdb-query-session-display-status ()
  "Display status of all sessions."
  (interactive)
  (let ((sessions (duckdb-query-session-list)))
    (if (null sessions)
        (message "No active sessions")
      (with-help-window "*DuckDB Sessions*"
        (princ "DuckDB Sessions\n")
        (princ (make-string 50 ?=))
        (princ "\n\n")
        (dolist (session sessions)
          (princ (format "Session: %s\n" (duckdb-session-name session)))
          (princ (format "  Status:  %s\n" (duckdb-session-status session)))
          (princ (format "  Queries: %d\n" (duckdb-session-query-count session)))
          (princ (format "  Process: %s\n"
                         (if (process-live-p (duckdb-session-process session))
                             "alive" "dead")))
          (princ (format "  Created: %s\n"
                         (format-time-string "%H:%M:%S"
                                             (duckdb-session-created-at session))))
          (princ "\n"))))))

;;;###autoload
(defun duckdb-query-session-kill-interactive (name)
  "Interactively kill session NAME."
  (interactive
   (list (completing-read "Kill session: "
                          (mapcar #'duckdb-session-name
                                  (duckdb-query-session-list))
                          nil t)))
  (duckdb-query-session-kill name)
  (message "Killed session %s" name))

;;;###autoload
(defun duckdb-query-session-kill-all ()
  "Kill all sessions."
  (interactive)
  (let ((count 0))
    (maphash (lambda (name _session)
               (duckdb-query-session-kill name)
               (cl-incf count))
             (copy-hash-table duckdb-query-sessions))
    (message "Killed %d session(s)" count)))

;;;;;; Mode Integration Helper

(defun duckdb-query-session-setup-buffer (session-name)
  "Setup current buffer for session SESSION-NAME.

Creates session if needed and registers buffer as owner.
Sets `duckdb-query--current-session' buffer-locally.

Call from mode hooks:
  (add-hook \\='my-mode-hook
            (lambda ()
              (duckdb-query-session-setup-buffer \"my-session\")))"
  (unless (duckdb-query-session-get session-name)
    (duckdb-query-session-start session-name))
  (duckdb-query-session-register-owner session-name (current-buffer))
  (setq-local duckdb-query--current-session session-name)
  (duckdb-query-session-get session-name))

;;;;;; Database Attachment

(defun duckdb-query-session-attach (session-name database &optional alias readonly)
  "Attach DATABASE to session SESSION-NAME as ALIAS.

DATABASE is file path to DuckDB database file.

ALIAS is SQL identifier for the attached database.  When nil, defaults
to filename without extension, sanitized for SQL (non-alphanumeric
characters replaced with underscores).

READONLY when non-nil (default t) opens database in read-only mode,
preventing write conflicts.  Use nil for write access.

Returns alias string used for attachment.

Signals error if session does not exist or process is dead.

Within queries, access attached tables as ALIAS.table_name.

Example:

  (duckdb-query-session-attach \"work\" \"/data/sales.db\")
  ;; Returns \"sales\"
  ;; Access as: SELECT * FROM sales.orders

  (duckdb-query-session-attach \"work\" \"/data/app.db\" \"mydb\" nil)
  ;; Returns \"mydb\", writable
  ;; Access as: INSERT INTO mydb.logs VALUES (...)

Also see `duckdb-query-session-detach' to remove attachment.
Also see `duckdb-query-with-database' for scoped attachment."
  (let* ((session (or (gethash session-name duckdb-query-sessions)
                      (error "Session %s does not exist" session-name)))
         (proc (duckdb-session-process session))
         (raw-alias (or alias (file-name-base database)))
         (alias (replace-regexp-in-string "[^a-zA-Z0-9_]" "_" raw-alias))
         (readonly (if (memq readonly '(nil t)) (or readonly t) readonly)))
    (unless (process-live-p proc)
      (error "Session %s process is dead" session-name))
    (duckdb-query-session-execute
     session-name
     (format "ATTACH '%s' AS %s%s"
             (expand-file-name database)
             alias
             (if readonly " (READ_ONLY)" ""))
     10)
    alias))

(defun duckdb-query-session-detach (session-name alias)
  "Detach database ALIAS from session SESSION-NAME.

ALIAS is the identifier used when attaching the database via
`duckdb-query-session-attach' or `duckdb-query-with-database'.

Signals error if session does not exist or ALIAS is not attached.

After detaching, tables from the database are no longer accessible
via ALIAS.table_name syntax.

Example:

  (duckdb-query-session-attach \"work\" \"/data/sales.db\" \"sales\")
  ;; ... use sales.orders, sales.customers ...
  (duckdb-query-session-detach \"work\" \"sales\")
  ;; sales.* no longer accessible

Also see `duckdb-query-session-attach' for attaching databases.
Also see `duckdb-query-with-database' for automatic attach/detach."
  (duckdb-query-session-execute
   session-name
   (format "DETACH %s" alias)
   5))

;;;;; Function Executors

(cl-defmethod duckdb-query-execute ((executor function) query &rest args)
  "Execute QUERY by calling EXECUTOR function with QUERY and ARGS.

EXECUTOR must be a function accepting (query &rest args) and returning
JSON string.

This enables custom execution strategies without defining new executor
types.  The function receives QUERY as first argument followed by all
ARGS as keyword parameters.

Return JSON string from executor function.
Signal error if executor function signals error.

Example:

  (defun my-executor (query &rest args)
    (let ((db (plist-get args :database)))
      ;; Custom execution logic
      ...))

  (duckdb-query \"SELECT 1\" :executor #\\='my-executor :database \"test.db\")"
  (apply executor query args))

(cl-defmethod duckdb-query-execute ((executor symbol) query &rest args)
  "Execute QUERY by calling function named by EXECUTOR symbol.

EXECUTOR must be symbol naming function that accepts (query &rest args)
and returns JSON string.

This handles the case where users pass function names as symbols rather
than function objects.

Return JSON string from executor function.
Signal error if EXECUTOR is not a function or if function signals error.

Example:

  (duckdb-query \"SELECT 1\" :executor \\='my-executor)
  (duckdb-query \"SELECT 1\" :executor #\\='my-executor)"
  (unless (fboundp executor)
    (error "Symbol %S is not a function" executor))
  (apply executor query args))

;;;; Low-Level Execution
(defun duckdb-query-execute-raw (query &optional database _timeout)
  "Execute QUERY via DuckDB CLI and return raw JSON string.

This is the low-level execution primitive.  Most code should use
`duckdb-query' or `duckdb-query-execute' instead.

QUERY is SQL string.
DATABASE is optional path to database file; nil uses in-memory.
TIMEOUT is reserved for future use; currently ignored.

Return raw JSON output string for parsing.
Signal error on non-zero exit code.

Use `duckdb-query-executable' for subprocess invocation.
Wrapped by `duckdb-query-execute' `:cli' method."
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

;;;; Format Conversion
;;;;; Columnar Format

(defun duckdb-query--to-columnar (rows)
  "Convert ROWS to columnar format.

ROWS is list of alists from JSON parsing.

Return alist of (COLUMN-NAME . VECTOR) pairs where each vector
contains all values for that column.

Use hash table for O(1) column lookup and single-pass algorithm
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

;;;;; Org-Table Format

(defun duckdb-query--to-org-table (rows)
  "Convert ROWS to org-table format.

ROWS is list of alists from JSON parsing.

Return list of lists: first row is headers, remaining rows are values.
Each row is a list of column values in consistent order, with native
Lisp types (numbers, strings, symbols).

Use hash table for O(1) column lookup and minimize allocations.

Called by `duckdb-query' when FORMAT is `:org-table'."
  (when rows
    (let* ((headers (mapcar #'car (car rows)))
           (col-count (length headers))
           (col-index (cl-loop with ht = (make-hash-table :test 'equal :size col-count)
                               for col in headers
                               for idx from 0
                               do (puthash col idx ht)
                               finally return ht)))
      (cons headers
            (cl-loop for row in rows
                     collect (let ((row-vec (make-vector col-count nil)))
                               (dolist (cell row)
                                 (when-let ((idx (gethash (car cell) col-index)))
                                   (aset row-vec idx (cdr cell))))
                               (append row-vec nil)))))))

(defun duckdb-query-org-table-to-alist (org-table)
  "Convert ORG-TABLE to list of alists for use with :data parameter.

ORG-TABLE is list of lists where first row is headers (symbols or
strings) and remaining rows are data values.  This is the format
returned by `duckdb-query' with :format :org-table.

Returns list of alists suitable for `duckdb-query' :data parameter,
enabling roundtrip workflows where query results become inputs to
subsequent queries.

Example:

  (duckdb-query-org-table-to-alist
   \\='((id name score)
     (1 \"Alice\" 95)
     (2 \"Bob\" 87)))
  ;; => (((id . 1) (name . \"Alice\") (score . 95))
  ;;     ((id . 2) (name . \"Bob\") (score . 87)))

Also see `duckdb-query' with :format :org-table."
  (let ((headers (mapcar (lambda (h)
                           (if (symbolp h) h (intern h)))
                         (car org-table))))
    (mapcar (lambda (row)
              (cl-mapcar #'cons headers row))
            (cdr org-table))))

(defun duckdb-query--resolve-org-ref (ref-string)
  "Resolve @org:NAME or @org:FILE:NAME reference to alist data.

REF-STRING is the reference without @org: prefix.
Returns alist data suitable for serialization.

Examples:
  \"my-table\" - resolve named table in current buffer
  \"path/to/file.org:my-table\" - resolve table in specific file

Uses `org-babel-ref-resolve' for table lookup.
Uses `duckdb-query-org-table-to-alist' for format conversion.
Called by `duckdb-query--substitute-org-refs'."
  (let* ((parts (split-string ref-string ":"))
         (name (car (last parts)))
         (file (when (> (length parts) 1)
                 (string-join (butlast parts) ":"))))
    (save-excursion
      (save-restriction
        (widen)
        (when file
          (let ((buf (find-file-noselect (expand-file-name file))))
            (set-buffer buf)))
        (let ((table-data (condition-case err
                              (org-babel-ref-resolve name)
                            (error
                             (error "Could not resolve org reference `%s': %s"
                                    ref-string (error-message-string err))))))
          (if (and (listp table-data)
                   (listp (car table-data))
                   table-data)
              ;; Filter out hline symbols before conversion
              (let ((clean-table (cl-remove-if (lambda (row) (eq row 'hline))
                                               table-data)))
                (duckdb-query-org-table-to-alist clean-table))
            (error "Reference `%s' did not resolve to table data (got %S)"
                   ref-string (type-of table-data))))))))


(defun duckdb-query--substitute-org-refs (query temp-files)
  "Replace @org:NAME references in QUERY with temp file paths.

QUERY is SQL string potentially containing @org:NAME references.
TEMP-FILES is hash table for cleanup tracking.

Returns modified query string with @org: references replaced.

Uses `duckdb-query--resolve-org-ref' for reference resolution.
Uses `duckdb-query--alist-to-json-file' for serialization.
Called by `duckdb-query'."
  (let ((result query)
        (org-ref-pattern "@org:\\([^ \t\n,)]+\\)"))
    (while (string-match org-ref-pattern result)
      (let* ((ref-string (match-string 1 result))
             (match-start (match-beginning 0))
             (match-end (match-end 0))
             (alist-data (duckdb-query--resolve-org-ref ref-string))
             (temp-file (make-temp-file
                         (format "duckdb-org-%s-"
                                 (replace-regexp-in-string "[/:]" "_" ref-string))
                         nil ".json")))
        (duckdb-query--alist-to-json-file alist-data temp-file)
        (puthash (intern (concat "org:" ref-string)) temp-file temp-files)
        (setq result (concat (substring result 0 match-start)
                             (format "'%s'" temp-file)
                             (substring result match-end)))))
    result))

;;;; Nested Type Preservation
(defun duckdb-query--nested-type-p (type-string)
  "Return non-nil if TYPE-STRING represents a nested DuckDB type.

Nested types include STRUCT, MAP, LIST (TYPE[]), and ARRAY (TYPE[N]).

TYPE-STRING is the column_type from DESCRIBE output.

Called by `duckdb-query--wrap-nested-columns'."
  (or (string-prefix-p "STRUCT" type-string)
      (string-prefix-p "MAP" type-string)
      (string-match-p "\\[\\]$" type-string)        ; LIST: TYPE[]
      (string-match-p "\\[[0-9]+\\]$" type-string))) ; ARRAY: TYPE[N]

(defun duckdb-query--wrap-nested-columns (query)
  "Wrap nested type columns in QUERY with to_json() for proper JSON output.

Detects STRUCT, MAP, LIST, and ARRAY columns via schema introspection
and wraps them with to_json() so they serialize as proper JSON
objects/arrays instead of DuckDB's string representations.

QUERY is SQL string to analyze and potentially wrap.

Returns modified query string with nested columns wrapped, or original
QUERY if no nested columns detected.

Example:
  Input:  \"SELECT struct_col, array_col, id FROM table\"
  Output: \"SELECT to_json(\\\"struct_col\\\") AS \\\"struct_col\\\",
                  to_json(\\\"array_col\\\") AS \\\"array_col\\\",
                  \\\"id\\\"
           FROM (...) AS _duckdb_nested_wrap\"

Called by `duckdb-query' when :preserve-nested is non-nil."
  (let* ((schema (condition-case nil
                     (duckdb-query-describe query)
                   (error nil)))
         (nested-cols
          (when schema
            (cl-remove-if-not
             (lambda (row)
               (duckdb-query--nested-type-p (cdr (assq 'column_type row))))
             schema)))
         (nested-names (mapcar (lambda (r) (cdr (assq 'column_name r))) nested-cols)))
    (if (or (null schema) (null nested-names))
        query
      ;; Wrap query with SELECT that converts nested columns
      (let* ((all-cols (mapcar (lambda (r) (cdr (assq 'column_name r))) schema))
             (select-exprs
              (mapcar (lambda (col)
                        (if (member col nested-names)
                            (format "to_json(\"%s\") AS \"%s\"" col col)
                          (format "\"%s\"" col)))
                      all-cols)))
        (format "SELECT %s FROM (%s) AS _duckdb_nested_wrap"
                (string-join select-exprs ", ")
                query)))))

(defun duckdb-query--prepare-for-json (value)
  "Prepare VALUE for `json-serialize' by converting lists to vectors.

`json-serialize' interprets lists as alists (key-value pairs).
Non-alist lists (arrays from DuckDB) must be converted to vectors
for correct JSON array serialization.

VALUE can be any Elisp value from DuckDB query results.

Recursively processes:
- Vectors: process each element
- Alist entries (symbol . value): preserve as cons, process value
- Plain lists (1 2 3): convert to vector [1 2 3]
- Atoms: pass through unchanged

Called by `duckdb-query--alist-to-json-file' when data contains
nested structures from queries with :preserve-nested."
  (cond
   ;; nil stays nil
   ((null value) value)
   ;; Special symbols pass through
   ((memq value '(:null :false t)) value)
   ;; Vectors: recursively process elements
   ((vectorp value)
    (vconcat (mapcar #'duckdb-query--prepare-for-json value)))
   ;; Cons cell
   ((consp value)
    (let ((first (car value)))
      (cond
       ;; Alist entry: (symbol . something) where symbol is not a special value
       ((and (symbolp first)
             (not (memq first '(:null :false t))))
        (let ((rest (cdr value)))
          (if (and (listp rest)
                   rest  ; not nil
                   (not (and (consp (car rest))
                             (symbolp (caar rest)))))
              ;; cdr is a list but NOT an alist - this is an array value like (arr 1 2 3)
              ;; Convert to (arr . [1 2 3])
              (cons first (vconcat (mapcar #'duckdb-query--prepare-for-json rest)))
            ;; cdr is either atomic or an alist - process recursively
            (cons first (duckdb-query--prepare-for-json rest)))))
       ;; List of alist entries (a row)
       ((and (consp first) (symbolp (car first)))
        (mapcar #'duckdb-query--prepare-for-json value))
       ;; Plain list of values - convert to vector
       (t
        (vconcat (mapcar #'duckdb-query--prepare-for-json value))))))
   ;; Everything else passes through
   (t value)))

(defun duckdb-query--alist-to-json-file (alist-data file)
  "Write ALIST-DATA to FILE as JSON array.

ALIST-DATA is list of alists where each alist represents a row
and each cons cell is (column . value).  Values may be nested
alists or lists from queries with :preserve-nested.

FILE is path to write JSON output.

Uses native `json-serialize' for performance (~2x faster than CSV,
~4x faster than `json-encode').  Handles :null and :false correctly.
Converts integers outside JSON safe range (±2^53) to strings to
avoid precision loss.

Nested structures (alists, lists) are processed by
`duckdb-query--prepare-for-json' to ensure correct JSON
array/object serialization.

Called by `duckdb-query--substitute-data-refs' for data serialization."
  (let* ((max-safe-int (expt 2 53))
         (min-safe-int (- (expt 2 53)))
         ;; First prepare nested structures (convert lists to vectors)
         (prepared-data (duckdb-query--prepare-for-json alist-data))
         ;; Then handle large integers
         (safe-data
          (if (vectorp prepared-data)
              (cl-map 'vector
                      (lambda (row)
                        (mapcar
                         (lambda (cell)
                           (let ((v (cdr cell)))
                             (if (and (integerp v)
                                      (or (> v max-safe-int)
                                          (< v min-safe-int)))
                                 (cons (car cell) (number-to-string v))
                               cell)))
                         row))
                      prepared-data)
            ;; Already a vector from prepare-for-json
            prepared-data)))
    (with-temp-file file
      (insert (json-serialize (if (vectorp safe-data) safe-data (vconcat safe-data))
                              :null-object :null
                              :false-object :false)))))

(defun duckdb-query--alist-to-csv-file (alist-data file)
  "Write ALIST-DATA to FILE as CSV.

ALIST-DATA is list of alists where each alist represents a row
and each cons cell is (column . value).

FILE is path to write CSV output.

Writes directly to file buffer, avoiding intermediate string
allocation.  Handles RFC 4180 CSV escaping: strings containing
commas, quotes, or newlines are quoted with doubled internal quotes.

Handles special Elisp values:
- :null → empty string (SQL NULL)
- :false → \"false\" (SQL FALSE)
- t → \"true\" (SQL TRUE)

Note: Nested structures (alists, lists) are serialized via
`prin1-to-string' and will not roundtrip correctly.  Use JSON
format (:data-format :json) for nested data.

Called by `duckdb-query--substitute-data-refs' when :data-format is :csv."
  (with-temp-file file
    (let ((headers (mapcar #'car (car alist-data))))
      ;; Header row
      (insert (mapconcat #'symbol-name headers ",") "\n")
      ;; Data rows
      (dolist (row alist-data)
        (insert
         (mapconcat
          (lambda (cell)
            (let ((v (cdr cell)))
              (cond
               ;; Special Elisp values
               ((eq v :null) "")
               ((eq v :false) "false")
               ((eq v t) "true")
               ;; Strings with RFC 4180 escaping
               ((stringp v)
                (if (string-match-p "[,\"\n]" v)
                    (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" v))
                  v))
               ;; nil treated as empty/NULL
               ((null v) "")
               ;; Everything else via prin1
               (t (prin1-to-string v)))))
          row ",")
         "\n")))))

(defun duckdb-query--substitute-data-refs (query data data-format temp-files)
  "Replace @SYMBOL references in QUERY with temp file paths.

DATA is the :data parameter in one of these forms:
  - List of alists: bound to @data implicitly
  - Alist of (SYMBOL . VALUE) pairs: symbols become @symbol references

Values must be actual alist data, not symbols to be evaluated.
Use backquote with comma to pass lexical variables:

  (let ((my-data \\='(((id . 1)))))
    (duckdb-query \"SELECT * FROM @data\" :data \\=`((data . ,my-data))))

DATA-FORMAT controls serialization:
  :json - JSON array via `json-serialize' (default, faster, better types)
  :csv  - CSV via `duckdb-query--alist-to-csv-file' (for compatibility)

TEMP-FILES is hash table mapping symbols to temp file paths for
cleanup tracking.

Return modified query string.

Examples:
  Single source (implicit @data):
    :data \\='(((id . 1) (name . \"Alice\")))

  Named bindings with backquote:
    :data \\=`((orders . ,my-orders-var))
    :data \\=`((orders . ,orders) (users . ,users))

Called by `duckdb-query' when DATA parameter is provided."
  (let ((result query)
        (serializer (if (eq data-format :csv)
                        #'duckdb-query--alist-to-csv-file
                      #'duckdb-query--alist-to-json-file))
        (file-ext (if (eq data-format :csv) ".csv" ".json"))
        (bindings
         (cond
          ;; nil -> no bindings
          ((null data)
           nil)

          ;; Direct data: list of alists -> bind to 'data
          ;; Detect by checking if first element looks like a row (alist)
          ;; rather than a binding (symbol . list-of-alists)
          ((and (listp data)
                (listp (car data))
                (consp (caar data)))
           `((data . ,data)))

          ;; List of bindings: each should be (symbol . alist-data)
          (t
           (mapcar
            (lambda (binding)
              (pcase binding
                (`(,(and sym (pred symbolp))
                   . ,(and value
                           (pred listp)
                           (pred (lambda (v)
                                   (and v
                                        (listp (car v))
                                        (consp (caar v)))))))
                 (cons sym value))
                (_
                 (error "Invalid data binding: %S.  Expected (symbol . alist-data); use backquote: \\=`((sym . ,var))" binding))))
            data)))))

    ;; Substitute each binding
    (pcase-dolist (`(,sym . ,alist-data) bindings)
      (let* ((pattern (format "@%s\\b" (regexp-quote (symbol-name sym))))
             (temp-file (make-temp-file
                         (format "duckdb-data-%s-" sym)
                         nil file-ext)))
        (funcall serializer alist-data temp-file)
        (puthash sym temp-file temp-files)
        (setq result (replace-regexp-in-string
                      pattern
                      (format "'%s'" temp-file)
                      result t t))))
    result))

;;;; Main Entry Point

(cl-defun duckdb-query (query &rest args &key
                              database
                              timeout
                              (format :alist)
                              (executor nil)
                              (output-via :file)
                              data
                              (data-format :json)
                              preserve-nested
                              &allow-other-keys)
  "Execute QUERY and return results in FORMAT.

QUERY is SQL string to execute.  May contain @SYMBOL references to
Elisp data provided via DATA parameter.

DATABASE is optional database file path.  When nil, uses
`duckdb-query-default-database' if set, otherwise in-memory database.

TIMEOUT is optional execution timeout in seconds.  Defaults to
`duckdb-query-default-timeout'.

FORMAT is output structure:
  :alist     - list of alists (default)
  :plist     - list of plists
  :hash      - list of hash-tables
  :vector    - vector of alists
  :columnar  - alist of column vectors
  :org-table - list of lists for `org-mode' tables
  :raw       - unprocessed output string from executor

EXECUTOR controls execution strategy:
  :cli       - Direct CLI invocation (default)
  function   - Custom executor function
  symbol     - Function name as symbol
  object     - Custom executor object with `cl-defmethod'

OUTPUT-VIA controls how results are transferred from DuckDB:
  :file      - Write to temp file via COPY (default, ~8x faster for
               large results, nested types serialize correctly)
  :pipe      - Stream through stdout pipe (required for DDL/DML,
               use with :preserve-nested for nested types)

When OUTPUT-VIA is `:file' and QUERY cannot be wrapped in
COPY \(DDL, DML,DESCRIBE statements), automatically falls back to `:pipe'.

DATA enables querying Elisp data structures via @SYMBOL references.
Values must be actual data, not symbols; use backquote for variables:

  Direct data (referenced as @data):
    :data \\='(((id . 1) (name . \"Alice\")) ...)

  Named bindings (use backquote with comma for variables):
    :data \\=`((orders . ,my-orders-var)
            (users . ,my-users-var))

DATA-FORMAT controls how DATA is serialized to temporary files:
  :json - JSON array via native `json-serialize' (default)
          Faster (~2x vs CSV), preserves nested types, handles
          :null and :false correctly.
  :csv  - CSV format for compatibility with tools expecting CSV.
          Use when joining with CSV files or for debugging.

PRESERVE-NESTED when non-nil and OUTPUT-VIA is `:pipe', wraps STRUCT,
MAP, LIST, and ARRAY columns with to_json() so they return as nested
Elisp structures instead of string representations.

When OUTPUT-VIA is `:file' (the default), nested types serialize
correctly without this parameter.  PRESERVE-NESTED is only needed
when explicitly using `:output-via :pipe'.

Additional keyword arguments in ARGS are passed to EXECUTOR.

Returns nil for empty results.
Returns converted data in FORMAT for successful queries.

QUERY may contain @org:NAME references to named org tables in current
buffer, resolved via `org-babel-ref-resolve'.  Use @org:FILE:NAME for
tables in other files.

Examples:

  ;; Simple query (uses fast file output by default)
  (duckdb-query \"SELECT 42 as answer\")
  ;; => (((answer . 42)))

  ;; Nested types work automatically with file output
  (duckdb-query \"SELECT {'x':1,'y':2}::STRUCT(x INT,y INT) as p\")
  ;; => (((p (x . 1) (y . 2))))

  ;; DDL falls back to pipe automatically
  (duckdb-query \"CREATE TABLE test (id INT)\" :readonly nil)

  ;; Explicit pipe mode with preserve-nested
  (duckdb-query \"SELECT * FROM nested_table\"
                :output-via :pipe
                :preserve-nested t)

Uses `duckdb-query-execute' for execution dispatch.
Uses `duckdb-query--substitute-data-refs' for @symbol replacement.
Uses `duckdb-query--substitute-org-refs' for @org: replacement."
  (let* ((executor (duckdb-query--resolve-executor executor))
         (temp-files (make-hash-table :test 'eq))
         ;; First: resolve @org: references
         (org-resolved-query (duckdb-query--substitute-org-refs query temp-files))
         ;; Then: substitute @data references
         (substituted-query (if data
                                (duckdb-query--substitute-data-refs
                                 org-resolved-query data data-format temp-files)
                              org-resolved-query))
         ;; Then: wrap for nested preservation (only for pipe mode)
         (effective-query (if (and preserve-nested
                                   (or (eq output-via :pipe)
                                       (eq executor :session)))
                              (duckdb-query--wrap-nested-columns substituted-query)
                            substituted-query))
         (db (or database duckdb-query-default-database))
         (clean-args (cl-loop for (k v) on args by #'cddr
                              unless (memq k '(:data :data-format :preserve-nested))
                              append (list k v)))
         (output nil))
    (unwind-protect
        (progn
          (setq output (apply #'duckdb-query-execute
                              executor
                              effective-query
                              :database db
                              :timeout timeout
                              :output-via output-via
                              clean-args))
          (when (and output (not (string-empty-p (string-trim output))))
            (let ((trimmed (string-trim output)))
              (cond
               ;; Raw format - return as-is
               ((eq format :raw)
                output)

               ;; Empty output
               ((string-empty-p trimmed)
                nil)

               ;; JSON array or object - parse according to format
               ((memq (aref trimmed 0) '(?\[ ?\{))
                (pcase format
                  (:alist
                   (json-parse-string output
                                      :object-type 'alist
                                      :array-type 'list
                                      :null-object duckdb-query-null-value
                                      :false-object duckdb-query-false-value))
                  (:plist
                   (json-parse-string output
                                      :object-type 'plist
                                      :array-type 'list
                                      :null-object duckdb-query-null-value
                                      :false-object duckdb-query-false-value))
                  (:hash
                   (json-parse-string output
                                      :object-type 'hash-table
                                      :array-type 'list
                                      :null-object duckdb-query-null-value
                                      :false-object duckdb-query-false-value))
                  (:vector
                   (json-parse-string output
                                      :object-type 'alist
                                      :array-type 'array
                                      :null-object duckdb-query-null-value
                                      :false-object duckdb-query-false-value))
                  (:columnar
                   (duckdb-query--to-columnar
                    (json-parse-string output
                                       :object-type 'alist
                                       :array-type 'list
                                       :null-object duckdb-query-null-value
                                       :false-object duckdb-query-false-value)))
                  (:org-table
                   (duckdb-query--to-org-table
                    (json-parse-string output
                                       :object-type 'alist
                                       :array-type 'list
                                       :null-object duckdb-query-null-value
                                       :false-object duckdb-query-false-value)))
                  (_
                   (error "Unknown format: %s.  Valid: :alist :plist :hash :vector :columnar :org-table :raw"
                          format))))

               ;; DuckDB error message - signal error
               ((string-match-p
                 "\\(?:Error\\|Exception\\|SYNTAX_ERROR\\|CATALOG_ERROR\\|BINDER_ERROR\\|PARSER_ERROR\\):"
                 (duckdb-query--strip-ansi trimmed))
                (error "DuckDB error: %s" (duckdb-query--strip-ansi trimmed)))

               ;; DDL/DML success - non-JSON, non-error output (e.g., prompt char "D")
               ;; Return nil to indicate successful execution with no result set
               (t
                nil)))))
      ;; Cleanup temp files
      (maphash (lambda (_sym file)
                 (when (file-exists-p file)
                   (delete-file file)))
               temp-files))))

(cl-defun duckdb-query-file (file &key database readonly (format :alist) (output-via :file))
  "Execute SQL from FILE and return results in FORMAT.

FILE is path to SQL file containing query.
DATABASE is optional database file path (nil for in-memory).
READONLY defaults to t when DATABASE specified.
FORMAT is output structure (:alist, :columnar, :org-table, etc.).
OUTPUT-VIA is output strategy (:file or :pipe), defaults to :file.

Return converted results in specified FORMAT.
Return nil for empty results.
Return raw output string for non-JSON results.

Use `duckdb-query' with SQL read from FILE.

Example:
  (duckdb-query-file \"queries/analysis.sql\"
                     :database \"data.db\"
                     :format :columnar)"
  (let ((sql (with-temp-buffer
               (insert-file-contents file)
               (buffer-string))))
    (duckdb-query sql
                  :database database
                  :readonly readonly
                  :format format
                  :output-via output-via)))

(provide 'duckdb-query)

;;; duckdb-query.el ends here
