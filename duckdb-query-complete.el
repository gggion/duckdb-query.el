;;; duckdb-query-complete.el --- Completion for duckdb-query forms -*- lexical-binding: t; -*-
;;
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

;; Completion-at-point support for `duckdb-query' SQL strings.
;;
;; Basic usage:
;;
;;     (add-hook 'emacs-lisp-mode-hook #'duckdb-query-complete-mode)
;;
;; Provides completion for @type:name references inside SQL string
;; arguments to `duckdb-query' and related functions.  Candidates
;; are extracted from the :sql, :data, and :val keyword parameters
;; via structural parsing.
;;
;; Completion triggers:
;; - @val:   Complete binding names from :val parameter
;; - @data:  Complete binding names from :data parameter
;; - @sql:   Complete binding names from :sql parameter
;; - @org:   No candidates (org tables resolved externally)
;; - @       Complete reference type prefixes
;;
;; Uses `duckdb-query-parse.el' for structural analysis.
;; Coexists with `elisp-completion-at-point' via :exclusive property.

;;; Code:

(require 'cl-lib)
(require 'duckdb-query-parse)
(require 'duckdb-query)

;;;; Customization

(defgroup duckdb-query-complete nil
  "Completion support for `duckdb-query' SQL strings."
  :group 'duckdb-query
  :prefix "duckdb-query-complete-")

(defcustom duckdb-query-complete-max-buffer-size nil
  "Maximum buffer size in bytes for completion activation.

When non-nil and buffer size exceeds this value,
`duckdb-query-complete-at-point' returns nil immediately.

Protects against performance degradation in large `org-mode' buffers
where `syntax-ppss' is expensive.

When nil, completion activates regardless of buffer size.

Also see `duckdb-query-font-lock-max-buffer-size'."
  :type '(choice (const :tag "No limit" nil)
                 (integer :tag "Maximum bytes"))
  :group 'duckdb-query-complete
  :package-version '(duckdb-query . "0.8.0"))

(defcustom duckdb-query-complete-sql-p t
  "Whether to offer SQL autocompletion via DuckDB.

When non-nil, `duckdb-query-complete-at-point' queries DuckDB's
metadata functions once at mode enable and serves SQL keyword,
function, and type candidates from a buffer-local cache.

When nil, only @type:name reference completion is offered.

Requires DuckDB CLI or active session for initial cache population.
Falls back gracefully on error, returning nil to allow other capf
functions to run.

Also see `duckdb-query-complete-at-point'.
Also see `duckdb-query-complete-refresh-cache'."
  :type 'boolean
  :group 'duckdb-query-complete
  :package-version '(duckdb-query . "0.8.0"))

;;;; Internal Variables
(defconst duckdb-query-complete--cache-query
  "SELECT label, type_label, priority FROM (
  SELECT DISTINCT keyword_name AS label,
    CASE WHEN keyword_category = 'reserved' THEN 'kw*' ELSE 'kw' END AS type_label,
    CASE WHEN keyword_category = 'reserved' THEN 100 ELSE 1000 END AS priority
  FROM duckdb_keywords()
  UNION ALL
  SELECT DISTINCT function_name AS label,
    CASE WHEN function_type = 'pragma' THEN 'pragma'
         WHEN function_type = 'aggregate' THEN 'fn/a'
         WHEN function_type = 'macro' THEN 'fn/m'
         WHEN function_type = 'table' THEN 'fn->T'
         WHEN function_type = 'table_macro' THEN 'fn/m->T'
         ELSE 'fn' END AS type_label,
    1000 AS priority
  FROM duckdb_functions()
  WHERE function_name NOT IN (SELECT keyword_name FROM duckdb_keywords())
  UNION ALL
  SELECT DISTINCT type_name AS label, 'type' AS type_label, 1000 AS priority
  FROM duckdb_types()
  WHERE database_name = 'system'
) ORDER BY priority, label"
  "SQL query to populate the completion cache.

Returns three columns: label, type_label, priority.
Combines keywords, functions, and types from DuckDB metadata.
Excludes function names that duplicate keyword names.
Sorts by priority (reserved keywords first) then alphabetically.

Type labels:
  kw*    - reserved SQL keyword
  kw     - unreserved SQL keyword
  fn     - scalar function
  fn/a   - aggregate function
  fn/m   - SQL macro
  fn->T  - table-producing function
  fn/m->T - table-producing macro
  pragma - pragma function
  type   - data type

Called by `duckdb-query-complete--populate-cache'.")

(defvar-local duckdb-query-complete--sql-cache nil
  "Cached SQL completion candidates for current buffer.

List of plists, each with :label, :type-label, and :priority keys.
Populated by `duckdb-query-complete--populate-cache'.
Invalidated by `duckdb-query-complete-refresh-cache'.

When nil, the cache has not been populated.  When the symbol `empty',
population was attempted but DuckDB was unavailable.

Also see `duckdb-query-complete-sql-source'.")

(defvar-local duckdb-query-complete--sql-candidates nil
  "Flat list of candidate strings derived from `duckdb-query-complete--sql-cache'.

Each string carries text properties:
  `duckdb-query--type-label' - category string (\"kw*\", \"fn\", \"agg\", etc.)
  `duckdb-query--priority'   - integer sort key (lower is higher priority)

Rebuilt by `duckdb-query-complete--populate-cache'.
Used by `duckdb-query-complete-at-point' for zero-latency filtering.")

;;;; Debugging Utilities
(defvar duckdb-query-complete--debug nil
  "When non-nil, log completion events to *duckdb-complete-debug* buffer.")

(defun duckdb-query-complete--debug-log (fmt &rest args)
  "Log formatted message to debug buffer when debugging is active.

FMT is a format string.  ARGS are format arguments.

Only logs when `duckdb-query-complete--debug' is non-nil.
Creates *duckdb-complete-debug* buffer on first use.

Called by `duckdb-query-complete-at-point' and
`duckdb-query-complete--sql-candidates'."
  (when duckdb-query-complete--debug
    (let ((buf (get-buffer-create "*duckdb-complete-debug*")))
      (with-current-buffer buf
        (goto-char (point-max))
        (insert (format-time-string "[%H:%M:%S] ")
                (apply #'format fmt args)
                "\n")))))

(defun duckdb-query-complete-toggle-debug ()
  "Toggle completion debug logging.

When enabled, all capf calls, SQL queries, errors, and candidate
lists are logged to the *duckdb-complete-debug* buffer."
  (interactive)
  (setq duckdb-query-complete--debug (not duckdb-query-complete--debug))
  (if duckdb-query-complete--debug
      (progn
        (get-buffer-create "*duckdb-complete-debug*")
        (message "duckdb-query completion debug ON"))
    (message "duckdb-query completion debug OFF")))

;;;; Reference Type Constants

(defconst duckdb-query-complete--type-strings '("sql" "data" "val" "org")
  "Valid reference type strings for @type:name references.")

(defconst duckdb-query-complete--type-candidates '("sql:" "data:" "val:" "org:")
  "Completion candidates for type selection after @ trigger.

Each candidate includes the trailing colon so cursor lands
immediately at the name position after completion.")

(defconst duckdb-query-complete--type-annotations
  '(("sql:"  . " SQL fragment")
    ("data:" . " Elisp data")
    ("val:"  . " literal value")
    ("org:"  . " Org table"))
  "Alist mapping type candidates to annotation strings.

Used by `duckdb-query-complete--type-annotation'.")

;;;; Context Detection

(defun duckdb-query-complete--ref-context-at-point ()
  "Detect @type:name reference context at point.

Return plist describing the completion context, or nil if point
is not at a reference trigger position.

Caller must verify point is inside a string within a recognized
`duckdb-query' form before calling this function.

Uses two-phase backward character scan:
1. Scan backward over name characters (a-z, A-Z, 0-9, _)
2. If preceded by colon, scan backward over type characters
3. If preceded by @, validate type against known types

Return plist with :context key indicating completion mode:

  (:context :type-name
   :type \"val\"
   :name-start 25
   :at-pos 20)

  (:context :type-prefix
   :type-start 21
   :at-pos 20)

  nil -- point is not at a reference trigger

Called by `duckdb-query-complete-at-point'.
Also see `duckdb-query-complete--type-strings' for valid types."
  (save-excursion
    (let ((end (point))
          name-start colon-pos type-start at-pos type-str)
      (skip-chars-backward "a-zA-Z0-9_")
      (setq name-start (point))
      (cond
       ;; Case 1: preceded by colon -- @type:name| context
       ((and (eq (char-before) ?:)
             (progn
               (setq colon-pos (1- (point)))
               (goto-char colon-pos)
               (skip-chars-backward "a-zA-Z")
               (setq type-start (point))
               (and (eq (char-before) ?@)
                    (setq at-pos (1- (point)))
                    (setq type-str (buffer-substring-no-properties
                                    type-start colon-pos))
                    (member type-str duckdb-query-complete--type-strings))))
        (list :context :type-name
              :type type-str
              :name-start name-start
              :at-pos at-pos))
       ;; Case 2: preceded by @ -- @type-prefix| context
       ((and (eq (char-before (point)) ?@)
             ;; Ensure name-start equals current point (no colon found)
             (= name-start (point))
             (setq at-pos (1- (point))))
        ;; Re-scan: name chars after @ with no colon
        (goto-char end)
        (skip-chars-backward "a-zA-Z0-9_")
        ;; Check if the character before the scan result is @
        (when (eq (char-before) ?@)
          (list :context :type-prefix
                :type-start (point)
                :at-pos (1- (point)))))
       ;; Case 2b: @ followed by partial type text
       ((progn
          (goto-char name-start)
          (and (> (- end name-start) 0)
               (eq (char-before) ?@)
               (setq at-pos (1- (point)))))
        (list :context :type-prefix
              :type-start name-start
              :at-pos at-pos))
       ;; Case 3: no @ trigger
       (t nil)))))

;;;; Boundary Validation

(defun duckdb-query-complete--in-completable-string-p (parse-result at-pos)
  "Return non-nil if AT-POS is in a completable string context.

PARSE-RESULT is a `duckdb-query-parse-result' struct.
AT-POS is the buffer position of the @ character.

Completable contexts:
- Main SQL string argument (between sql-beg and sql-end)
- String literals inside :val, :sql, or :data parameter values

Return non-nil when AT-POS falls within any of these regions.

Provides broader context coverage.

Called by `duckdb-query-complete-at-point'."
  (let ((sql-beg (duckdb-query-parse-result-sql-beg parse-result))
        (sql-end (duckdb-query-parse-result-sql-end parse-result)))
    (or
     ;; Main SQL string
     (and sql-beg sql-end
          (> at-pos sql-beg)
          (< at-pos sql-end))
     ;; Parameter value strings
     (cl-some
      (lambda (param)
        (let ((key (plist-get param :key)))
          (when (memq key '(:val :sql :data))
            (let ((val-beg (plist-get param :val-beg))
                  (val-end (plist-get param :val-end)))
              (and (> at-pos val-beg)
                   (< at-pos val-end))))))
      (duckdb-query-parse-result-params parse-result)))))

;;;; SQL Completion Cache

(defun duckdb-query-complete--populate-cache ()
  "Populate SQL completion cache from DuckDB metadata.

Query `duckdb_keywords', `duckdb_functions', and `duckdb_types'
in a single statement.  Store raw results in
`duckdb-query-complete--sql-cache' and build propertized string
list in `duckdb-query-complete--sql-candidates'.

For keywords (type-label starting with \"kw\"), generate both
uppercase and lowercase candidates so completion works regardless
of user's typing case.  Functions and types are stored as returned
by DuckDB (typically lowercase for functions, uppercase for types).

Uses `duckdb-query-default-database' and session context if
available.  Falls back to in-memory DuckDB when no database is
configured.

On error (DuckDB unavailable, network failure), set cache to
symbol `empty' and log to debug buffer.  Does not signal errors.

Called by `duckdb-query-complete-mode' on enable and by
`duckdb-query-complete-refresh-cache' interactively."
  (duckdb-query-complete--debug-log "populating SQL cache...")
  (condition-case err
      (let* ((rows (duckdb-query duckdb-query-complete--cache-query
                                 :format :alist))
             (candidates nil)
             (seen (make-hash-table :test 'equal)))
        (dolist (row rows)
          (let* ((label (cdr (assq 'label row)))
                 (type-label (cdr (assq 'type_label row)))
                 (priority (cdr (assq 'priority row))))
            ;; Add the original form
            (unless (gethash label seen)
              (puthash label t seen)
              (let ((candidate (copy-sequence label)))
                (put-text-property 0 (length candidate)
                                   'duckdb-query--type-label type-label
                                   candidate)
                (put-text-property 0 (length candidate)
                                   'duckdb-query--priority priority
                                   candidate)
                (push candidate candidates)))
            ;; For keywords, also add the other case
            (when (string-prefix-p "kw" type-label)
              (let ((alt (if (equal label (upcase label))
                             (downcase label)
                           (upcase label))))
                (unless (gethash alt seen)
                  (puthash alt t seen)
                  (let ((candidate (copy-sequence alt)))
                    (put-text-property 0 (length candidate)
                                       'duckdb-query--type-label type-label
                                       candidate)
                    (put-text-property 0 (length candidate)
                                       'duckdb-query--priority priority
                                       candidate)
                    (push candidate candidates)))))))
        (setq duckdb-query-complete--sql-cache rows)
        (setq duckdb-query-complete--sql-candidates (nreverse candidates))
        (duckdb-query-complete--debug-log
         "SQL cache populated: %d candidates" (length candidates)))
    (error
     (duckdb-query-complete--debug-log "SQL cache population failed: %S" err)
     (setq duckdb-query-complete--sql-cache 'empty)
     (setq duckdb-query-complete--sql-candidates nil))))

(defun duckdb-query-complete-refresh-cache ()
  "Refresh the SQL completion cache from DuckDB metadata.

Re-query `duckdb_keywords', `duckdb_functions', and `duckdb_types'
and rebuild the candidate list.  Use after loading extensions or
attaching databases that add new functions or types.

Also see `duckdb-query-complete--populate-cache'."
  (interactive)
  (duckdb-query-complete--populate-cache)
  (if (eq duckdb-query-complete--sql-cache 'empty)
      (message "DuckDB SQL cache: population failed (see debug buffer)")
    (message "DuckDB SQL cache: %d candidates"
             (length duckdb-query-complete--sql-candidates))))


;;;; SQL Autocompletion
(defun duckdb-query-complete--sql-annotation (candidate)
  "Return annotation for SQL CANDIDATE from cached metadata.

Read `duckdb-query--type-label' text property and format as
right-aligned category label.

Return \" kw*\" for reserved keywords, \" fn\" for scalar
functions, \" agg\" for aggregates, etc.

Falls back to duck emoji when candidate has no type property
\(live mode candidates).

Called as :annotation-function in SQL completion branch."
  (let ((type-label (get-text-property 0 'duckdb-query--type-label
                                       candidate)))
    (if type-label
        (format " %s" type-label)
      " ddb")))

(defun duckdb-query-complete--sql-sort (candidates)
  "Sort CANDIDATES by priority then alphabetically.

Read `duckdb-query--priority' text property from each candidate.
Lower priority values sort first (reserved keywords before functions).
Candidates without priority sort last.

Called as :display-sort-function in SQL completion branch."
  (sort (copy-sequence candidates)
        (lambda (a b)
          (let ((pa (or (get-text-property 0 'duckdb-query--priority a) 9999))
                (pb (or (get-text-property 0 'duckdb-query--priority b) 9999)))
            (if (= pa pb)
                (string< a b)
              (< pa pb))))))

(defun duckdb-query-complete--cached-sql-candidates ()
  "Return cached SQL candidate list, populating if needed.

If cache is nil, attempt population.  If cache is `empty' or
population fails, return nil.

Return `duckdb-query-complete--sql-candidates' list.

Called by `duckdb-query-complete-at-point' in :cache mode."
  (when (null duckdb-query-complete--sql-cache)
    (duckdb-query-complete--populate-cache))
  (unless (eq duckdb-query-complete--sql-cache 'empty)
    duckdb-query-complete--sql-candidates))

;;;; SQL Autocompcompletion - Static Analysis

(defun duckdb-query-complete--extract-cte-names (parse-result)
  "Extract CTE names from the SQL string in PARSE-RESULT.

Scan the main SQL string for WITH ... AS patterns and collect
the CTE names as completion candidates.

Return list of propertized candidate strings with type-label \"cte\".

Called by `duckdb-query-complete-at-point' for static enrichment."
  (let ((sql-beg (duckdb-query-parse-result-sql-beg parse-result))
        (sql-end (duckdb-query-parse-result-sql-end parse-result))
        (names nil))
    (when (and sql-beg sql-end)
      (save-excursion
        (goto-char sql-beg)
        ;; Match: WITH name AS or , name AS
        (while (re-search-forward
                "\\(?:\\bWITH\\b\\|,\\)\\s-+\\([a-zA-Z_][a-zA-Z0-9_]*\\)\\s-+\\bAS\\b"
                sql-end t)
          (let* ((name (match-string-no-properties 1))
                 (candidate (copy-sequence name)))
            (put-text-property 0 (length candidate)
                               'duckdb-query--type-label "cte"
                               candidate)
            (put-text-property 0 (length candidate)
                               'duckdb-query--priority 50
                               candidate)
            (push candidate names)))))
    (nreverse names)))

(defun duckdb-query-complete--extract-alias-names (parse-result)
  "Extract table aliases from the SQL string in PARSE-RESULT.

Scan for FROM/JOIN ... alias patterns where alias is not a keyword.
Return list of propertized candidate strings with type-label \"alias\".

Called by `duckdb-query-complete-at-point' for static enrichment."
  (let ((sql-beg (duckdb-query-parse-result-sql-beg parse-result))
        (sql-end (duckdb-query-parse-result-sql-end parse-result))
        (names nil)
        ;; Common keywords that appear after table refs but are not aliases
        (keywords '("WHERE" "ON" "JOIN" "LEFT" "RIGHT" "INNER" "OUTER"
                    "CROSS" "FULL" "GROUP" "ORDER" "HAVING" "LIMIT"
                    "UNION" "INTERSECT" "EXCEPT" "SET" "VALUES"
                    "QUALIFY" "WINDOW" "USING" "AS" "SELECT")))
    (when (and sql-beg sql-end)
      (save-excursion
        (goto-char sql-beg)
        ;; Match: FROM/JOIN <source> <alias>
        ;; where <source> can be identifier, @ref, or (...) subquery
        (while (re-search-forward
                "\\b\\(?:FROM\\|JOIN\\)\\s-+\\S-+\\s-+\\([a-zA-Z_][a-zA-Z0-9_]*\\)\\b"
                sql-end t)
          (let ((name (match-string-no-properties 1)))
            (unless (member (upcase name) keywords)
              (let ((candidate (copy-sequence name)))
                (put-text-property 0 (length candidate)
                                   'duckdb-query--type-label "alias"
                                   candidate)
                (put-text-property 0 (length candidate)
                                   'duckdb-query--priority 50
                                   candidate)
                (push candidate names)))))))
    (nreverse names)))

(defun duckdb-query-complete--extract-data-table-names (parse-result)
  "Extract @data: binding names as table-like candidates.

PARSE-RESULT is a `duckdb-query-parse-result' struct.

Return list of propertized candidate strings with type-label \"data\".
These are the names that appear after FROM/JOIN as @data:name or @name.

Called by `duckdb-query-complete-at-point' for static enrichment."
  (let ((bindings (cdr (assq :data
                              (duckdb-query-parse-result-bindings
                               parse-result))))
        (names nil))
    (dolist (sym bindings)
      (let* ((name (symbol-name sym))
             (candidate (copy-sequence name)))
        (put-text-property 0 (length candidate)
                           'duckdb-query--type-label "data"
                           candidate)
        (put-text-property 0 (length candidate)
                           'duckdb-query--priority 50
                           candidate)
        (push candidate names)))
    (nreverse names)))

(defun duckdb-query-complete--static-candidates (parse-result)
  "Collect all static analysis candidates from PARSE-RESULT.

Merge CTE names, table aliases, and @data: binding names into
a single list of propertized candidates.

Called by `duckdb-query-complete-at-point' in the SQL branch."
  (append (duckdb-query-complete--extract-cte-names parse-result)
          (duckdb-query-complete--extract-alias-names parse-result)
          (duckdb-query-complete--extract-data-table-names parse-result)))

;;;; Candidate Generation

(defun duckdb-query-complete--extract-binding-definitions (val-beg val-end)
  "Extract binding name-to-definition alist from parameter value.

VAL-BEG and VAL-END delimit the parameter value region.

Walk the alist structure, extracting each binding's name symbol
and the text representation of its value (the cdr of each cons cell).

Return alist of (NAME-STRING . DEFINITION-STRING) pairs.
Return nil if the region does not contain a valid binding list.

Handles quoted and backquoted forms.  Skips unquote markers
\(comma and comma-at).  Recognizes cons pairs by the dot separator
after the car symbol.

Example for :val \\='((min_price . 25) (avg_revenue . (sql \"...\"))):

  ((\"min_price\" . \"25\")
   (\"avg_revenue\" . \"(sql \\\"...\\\")\"))

Called by `duckdb-query-complete--binding-definitions'.
Uses the same structural walking strategy as
`duckdb-query--extract-binding-names'."
  (save-excursion
    (goto-char val-beg)
    (duckdb-query--skip-whitespace-and-comments)
    ;; Skip quote or backquote
    (when (memq (char-after) (list duckdb-query--char-quote
                                   duckdb-query--char-backquote))
      (forward-char 1))
    (let (definitions)
      (when (eq (char-after) duckdb-query--char-lparen)
        (let ((list-end (save-excursion
                          (when (duckdb-query--forward-sexp-safe)
                            (1- (point))))))
          (when (and list-end (< list-end val-end))
            (forward-char 1) ;; enter outer list
            (while (< (point) list-end)
              (duckdb-query--skip-whitespace-and-comments)
              ;; Skip unquote markers
              (when (eq (char-after) duckdb-query--char-comma)
                (forward-char 1)
                (when (eq (char-after) duckdb-query--char-at)
                  (forward-char 1)))
              (duckdb-query--skip-whitespace-and-comments)
              (when (and (< (point) list-end)
                         (eq (char-after) duckdb-query--char-lparen))
                (let ((pair-start (point)))
                  (forward-char 1) ;; enter cons cell
                  (duckdb-query--skip-whitespace-and-comments)
                  (when (looking-at "\\([a-zA-Z_][a-zA-Z0-9_-]*\\)")
                    (let ((name (match-string-no-properties 1)))
                      (goto-char (match-end 0))
                      (duckdb-query--skip-whitespace-and-comments)
                      (when (eq (char-after) duckdb-query--char-dot)
                        (forward-char 1) ;; skip dot
                        (duckdb-query--skip-whitespace-and-comments)
                        ;; Skip unquote marker before value
                        (when (eq (char-after) duckdb-query--char-comma)
                          (forward-char 1)
                          (when (eq (char-after) duckdb-query--char-at)
                            (forward-char 1)))
                        (duckdb-query--skip-whitespace-and-comments)
                        (let ((val-start (point)))
                          (when (duckdb-query--forward-sexp-safe)
                            (let ((val-text
                                   (string-trim
                                    (buffer-substring-no-properties
                                     val-start (point)))))
                              (push (cons name val-text)
                                    definitions)))))))
                  (goto-char pair-start)))
              (unless (duckdb-query--forward-sexp-safe)
                (forward-char 1))))))
      (nreverse definitions))))

(defun duckdb-query-complete--binding-definitions (parse-result type-str)
  "Return alist of (NAME . DEFINITION) for TYPE-STR from PARSE-RESULT.

PARSE-RESULT is a `duckdb-query-parse-result' struct.
TYPE-STR is one of \"sql\", \"data\", \"val\".

Return alist of (NAME-STRING . DEFINITION-STRING) pairs, or nil
if no bindings exist for that type.

Uses `duckdb-query-complete--extract-binding-definitions' to walk
the parameter value region.

Called by `duckdb-query-complete-at-point' for affixation."
  (let ((keyword (intern (format ":%s" type-str))))
    (cl-some (lambda (param)
               (when (eq (plist-get param :key) keyword)
                 (duckdb-query-complete--extract-binding-definitions
                  (plist-get param :val-beg)
                  (plist-get param :val-end))))
             (duckdb-query-parse-result-params parse-result))))

(defun duckdb-query-complete--binding-candidates (parse-result type-str)
  "Return binding name strings for TYPE-STR from PARSE-RESULT.

PARSE-RESULT is a `duckdb-query-parse-result' struct.
TYPE-STR is one of \"sql\", \"data\", \"val\", \"org\".

Return list of name strings from the corresponding binding keyword,
or nil if no bindings exist for that type.

For \"org\" type, always returns nil (org references are resolved
from external buffers, not from parsed bindings).

Called by `duckdb-query-complete-at-point'.
Uses `duckdb-query-parse-result-bindings' for data access."
  (let ((keyword (intern (format ":%s" type-str))))
    (mapcar #'symbol-name
            (cdr (assq keyword
                       (duckdb-query-parse-result-bindings parse-result))))))

;;;; Annotation Functions

(defun duckdb-query-complete--name-affixation (type-str definitions)
  "Return affixation function for binding name candidates.

TYPE-STR is the reference type (\"sql\", \"data\", \"val\").
DEFINITIONS is alist of (NAME . DEFINITION-TEXT) pairs.

Return function suitable for :affixation-function property.
Each candidate is displayed as:

  candidate-name  definition-text  @type

Called by `duckdb-query-complete-at-point'."
  (let ((type-suffix (format " @%s" type-str)))
    (lambda (candidates)
      (mapcar (lambda (cand)
                (let ((def (or (cdr (assoc cand definitions)) "")))
                  ;; Truncate long definitions
                  (when (> (length def) 60)
                    (setq def (concat (substring def 0 57) "...")))
                  (list cand "" (propertize (format " %s %s" def type-suffix)
                                            'face 'completions-annotations))))
              candidates))))

(defun duckdb-query-complete--type-annotation (candidate)
  "Return annotation for type CANDIDATE.

CANDIDATE is a string like \"sql:\", \"data:\", etc.

Return description string from `duckdb-query-complete--type-annotations'.

Used as :annotation-function for type prefix completion.
Called by `duckdb-query-complete-at-point'."
  (or (cdr (assoc candidate duckdb-query-complete--type-annotations))
      ""))

;;;; Org Mode Integration

(defun duckdb-query-complete--org-src-block-bounds ()
  "Return (BEG . END) of elisp src block body at point, or nil.

BEG is position after #+begin_src line.
END is position of #+end_src line.

Only matches emacs-lisp and elisp src blocks.
Returns nil if point is not inside such a block."
  (when (derived-mode-p 'org-mode)
    (save-excursion
      (let ((pos (point)))
        (when (re-search-backward
               "^[ \t]*#\\+begin_src[ \t]+\\(?:emacs-lisp\\|elisp\\)\\b.*$"
               nil t)
          (let ((body-beg (1+ (match-end 0))))
            (when (re-search-forward
                   "^[ \t]*#\\+end_src[ \t]*$"
                   nil t)
              (let ((body-end (match-beginning 0)))
                (when (and (<= body-beg pos)
                           (<= pos body-end))
                  (cons body-beg body-end))))))))))

(defun duckdb-query-complete--in-org-src-block ()
  "Attempt completion inside an org elisp src block.

Narrow to the src block body, switch to `emacs-lisp-mode-syntax-table',
and delegate to the standard completion logic.

Returns capf result or nil."
  (when-let ((bounds (duckdb-query-complete--org-src-block-bounds)))
    (let ((block-beg (car bounds))
          (block-end (cdr bounds)))
      (save-restriction
        (narrow-to-region block-beg block-end)
        (with-syntax-table emacs-lisp-mode-syntax-table
          ;; Now syntax-ppss and list walking operate on pure Elisp
          ;; within a small region
          (when (nth 3 (syntax-ppss))
            (when-let ((parse-result (duckdb-query--parse-at-point)))
              (let ((ref-ctx (duckdb-query-complete--ref-context-at-point)))
                (duckdb-query-complete--dispatch
                 parse-result ref-ctx)))))))))

;;;; Dispatch Logic
(defun duckdb-query-complete--dispatch (parse-result ref-ctx)
  "Dispatch completion based on PARSE-RESULT and REF-CTX.

PARSE-RESULT is a `duckdb-query-parse-result' struct.
REF-CTX is a reference context plist from
`duckdb-query-complete--ref-context-at-point', or nil.

Return capf result list or nil.

Called by `duckdb-query-complete-at-point' and
`duckdb-query-complete--in-org-src-block'."
  (duckdb-query-complete--debug-log "dispatch ref-ctx=%S" ref-ctx)
  (if ref-ctx
      ;; Reference completion branch
      (let ((context (plist-get ref-ctx :context)))
        (pcase context
          ;; Context 1: @type:name -- complete binding names
          (:type-name
           (let ((type-str (plist-get ref-ctx :type))
                 (name-start (plist-get ref-ctx :name-start))
                 (at-pos (plist-get ref-ctx :at-pos)))
             (when (duckdb-query-complete--in-completable-string-p
                    parse-result at-pos)
               (let ((candidates
                      (duckdb-query-complete--binding-candidates
                       parse-result type-str)))
                 (duckdb-query-complete--debug-log
                  "ref @%s: candidates=%S" type-str candidates)
                 (when candidates
                   (let ((definitions
                          (duckdb-query-complete--binding-definitions
                           parse-result type-str)))
                     (list name-start (point) candidates
                           :exclusive t
                           :company-prefix-length t
                           :affixation-function
                           (duckdb-query-complete--name-affixation
                            type-str definitions))))))))
          ;; Context 2: @type-prefix -- complete type names
          (:type-prefix
           (let ((type-start (plist-get ref-ctx :type-start))
                 (at-pos (plist-get ref-ctx :at-pos)))
             (when (duckdb-query-complete--in-completable-string-p
                    parse-result at-pos)
               (duckdb-query-complete--debug-log "type-prefix completion")
               (list type-start (point)
                     duckdb-query-complete--type-candidates
                     :exclusive t
                     :company-prefix-length t
                     :annotation-function
                     #'duckdb-query-complete--type-annotation))))))
    ;; SQL completion branch
    (when duckdb-query-complete-sql-p
      (let* ((sql-beg (duckdb-query-parse-result-sql-beg
                       parse-result))
             (sql-end (duckdb-query-parse-result-sql-end
                       parse-result)))
        ;; Only complete in main SQL string
        (when (and sql-beg sql-end
                   (> (point) sql-beg)
                   (< (point) sql-end))
          (let* ((cached (duckdb-query-complete--cached-sql-candidates))
                 (static (duckdb-query-complete--static-candidates
                          parse-result))
                 (candidates (if static
                                 (append static cached)
                               cached)))
            (when candidates
              (let ((word-start
                     (save-excursion
                       (skip-chars-backward "a-zA-Z0-9_.")
                       (point))))
                (duckdb-query-complete--debug-log
                 "SQL cache: %d cached + %d static, start=%d end=%d"
                 (length (or cached '()))
                 (length (or static '()))
                 word-start (point))
                (list word-start (point) candidates
                      :exclusive 'no
                      :company-prefix-length t
                      :annotation-function
                      #'duckdb-query-complete--sql-annotation
                      :display-sort-function
                      #'duckdb-query-complete--sql-sort)))))))))

;;;; Main capf Entry Point
(defun duckdb-query-complete-at-point ()
  "Completion-at-point function for `duckdb-query' SQL strings.

Return nil when point is not in a completable context.
Return (START END COLLECTION . PROPS) for active completion.

In `emacs-lisp-mode', detect string context via `syntax-ppss'
and parse the enclosing `duckdb-query' form.

In `org-mode', narrow to the current elisp src block body and
switch to `emacs-lisp-mode-syntax-table' before parsing.  This
avoids scanning the entire org buffer and ensures correct syntax
context for list walking and `syntax-ppss'.

Skip entirely when buffer size exceeds
`duckdb-query-complete-max-buffer-size'.

Completion contexts, checked in order:

  @type:partial-name  Complete binding names for that type.
  @partial-type       Complete reference type prefixes.
  plain SQL text      Complete SQL keywords, functions, types.

Install via `duckdb-query-complete-mode' or manually:

  (add-hook \\='completion-at-point-functions
            #\\='duckdb-query-complete-at-point -90 t)

Uses `duckdb-query-complete--dispatch' for candidate generation.
Uses `duckdb-query-complete--in-org-src-block' for `org-mode' context.
Also see `duckdb-query-complete-toggle-debug' for debug logging."
  (duckdb-query-complete--debug-log "capf called at point=%d" (point))
  ;; Buffer size guard
  (when (or (null duckdb-query-complete-max-buffer-size)
            (<= (buffer-size) duckdb-query-complete-max-buffer-size))
    (cond
     ;; Org-mode: narrow to src block, use elisp syntax table
     ((derived-mode-p 'org-mode)
      (duckdb-query-complete--in-org-src-block))
     ;; Emacs-lisp-mode: standard path
     ((nth 3 (syntax-ppss))
      (when-let ((parse-result (duckdb-query--parse-at-point)))
        (let ((ref-ctx (duckdb-query-complete--ref-context-at-point)))
          (duckdb-query-complete--dispatch parse-result ref-ctx)))))))

;;;; Minor Mode

;;;###autoload
(define-minor-mode duckdb-query-complete-mode
  "Completion for @type:name references in `duckdb-query' forms.

When enabled, `completion-at-point' offers binding names as
candidates when typing @type:name references inside SQL string
arguments to `duckdb-query' and related functions.

When `duckdb-query-complete-sql-p' is non-nil, populate the SQL
completion cache from DuckDB metadata on enable.  Use
`duckdb-query-complete-refresh-cache' to update after loading
extensions or attaching databases.

Works with corfu, company-mode, and default completion UI.

Installed at depth -90 in `completion-at-point-functions' to run
before `elisp-completion-at-point', which rejects string contexts.

To enable globally:

    (add-hook \\='emacs-lisp-mode-hook #\\='duckdb-query-complete-mode)

Also see `duckdb-query-font-lock-mode' for reference highlighting.
Also see `duckdb-query-complete-at-point' for the capf function."
  :lighter nil
  :group 'duckdb-query-complete
  (if duckdb-query-complete-mode
      (progn
        (add-hook 'completion-at-point-functions
                  #'duckdb-query-complete-at-point -90 t)
        (when (and duckdb-query-complete-sql-p
                   (null duckdb-query-complete--sql-cache))
          (duckdb-query-complete--populate-cache)))
    (remove-hook 'completion-at-point-functions
                 #'duckdb-query-complete-at-point t)))

(provide 'duckdb-query-complete)

;;; duckdb-query-complete.el ends here
