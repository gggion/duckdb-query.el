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

;;;; Customization

(defgroup duckdb-query-complete nil
  "Completion support for `duckdb-query' SQL strings."
  :group 'duckdb-query
  :prefix "duckdb-query-complete-")

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

;;;; Candidate Generation

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

(defun duckdb-query-complete--name-annotation (type-str)
  "Return annotation function for binding name candidates.

TYPE-STR is the reference type string (\"sql\", \"data\", \"val\").

Return function suitable for :annotation-function property.

Called by `duckdb-query-complete-at-point'."
  (let ((suffix (format " @%s" type-str)))
    (lambda (_candidate) suffix)))

(defun duckdb-query-complete--type-annotation (candidate)
  "Return annotation for type CANDIDATE.

CANDIDATE is a string like \"sql:\", \"data:\", etc.

Return description string from `duckdb-query-complete--type-annotations'.

Used as :annotation-function for type prefix completion.
Called by `duckdb-query-complete-at-point'."
  (or (cdr (assoc candidate duckdb-query-complete--type-annotations))
      ""))

;;;; Main capf Entry Point

(defun duckdb-query-complete-at-point ()
  "Completion-at-point function for `duckdb-query' SQL strings.

Return nil when point is not in a completable context.
Return (START END COLLECTION . PROPS) for active completion.

Completion contexts:

  @type:partial-name  Complete binding names for that type.
  @partial-type       Complete reference type prefixes.

Completable string positions:
- Main SQL string argument to `duckdb-query' family functions
- String literals inside :val, :sql, and :data parameter values

Fast rejection path:
1. Not inside a string -- return nil immediately.
2. Not inside a `duckdb-query' family form -- return nil.
3. Not at an @ reference trigger -- return nil.
4. @ position not in a completable string -- return nil.

Coexists with `elisp-completion-at-point' via :exclusive property
set to \\='no, allowing fallback to other capf functions when this
function has no applicable candidates.

Install via `duckdb-query-complete-mode' or manually:

  (add-hook \\='completion-at-point-functions
            #\\='duckdb-query-complete-at-point -90 t)

Uses `duckdb-query-complete--ref-context-at-point' for trigger detection.
Uses `duckdb-query--parse-at-point' for structural analysis.
Uses `duckdb-query-complete--in-completable-string-p' for boundary check.
Uses `duckdb-query-complete--binding-candidates' for candidate extraction."
  ;; Fast rejection: must be inside a string
  (when (nth 3 (syntax-ppss))
    ;; Must be inside a duckdb-query family form
    (when-let* ((parse-result (duckdb-query--parse-at-point))
                (ref-ctx (duckdb-query-complete--ref-context-at-point)))
      (let ((context (plist-get ref-ctx :context)))
        (pcase context
          ;; Context 1: @type:name -- complete binding names
          (:type-name
           (let ((type-str (plist-get ref-ctx :type))
                 (name-start (plist-get ref-ctx :name-start))
                 (at-pos (plist-get ref-ctx :at-pos)))
             (when (duckdb-query-complete--in-completable-string-p
                    parse-result at-pos)
               (let ((candidates (duckdb-query-complete--binding-candidates
                                  parse-result type-str)))
                 (when candidates
                   (list name-start (point) candidates
                         :exclusive 'no
                         :annotation-function
                         (duckdb-query-complete--name-annotation
                          type-str)))))))
          ;; Context 2: @type-prefix -- complete type names
          (:type-prefix
           (let ((type-start (plist-get ref-ctx :type-start))
                 (at-pos (plist-get ref-ctx :at-pos)))
             (when (duckdb-query-complete--in-completable-string-p
                    parse-result at-pos)
               (list type-start (point)
                     duckdb-query-complete--type-candidates
                     :exclusive 'no
                     :annotation-function
                     #'duckdb-query-complete--type-annotation)))))))))

;;;; Minor Mode

;;;###autoload
(define-minor-mode duckdb-query-complete-mode
  "Completion for @type:name references in `duckdb-query' forms.

When enabled, `completion-at-point' offers binding names as
candidates when typing @type:name references inside SQL string
arguments to `duckdb-query' and related functions.

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
      (add-hook 'completion-at-point-functions
                #'duckdb-query-complete-at-point -90 t)
    (remove-hook 'completion-at-point-functions
                 #'duckdb-query-complete-at-point t)))

(provide 'duckdb-query-complete)

;;; duckdb-query-complete.el ends here
