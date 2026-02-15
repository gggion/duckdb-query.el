;;; duckdb-query-edit.el --- Editing utilities for duckdb-query forms -*- lexical-binding: t; -*-
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

;; Structural editing utilities for `duckdb-query' forms.
;;
;; Extract inline SQL text into parameterized references:
;;
;;     ;; Select a region inside a query string, then:
;;     M-x duckdb-query-edit-extract-to-ref
;;
;;     ;; Or bind specific types:
;;     (define-key my-map (kbd "C-c v") #'duckdb-query-edit-extract-to-val)
;;     (define-key my-map (kbd "C-c s") #'duckdb-query-edit-extract-to-sql)
;;
;; The primitives compose for programmatic use:
;;
;;     (duckdb-query-edit--insert-binding :val 'threshold "100")
;;     (duckdb-query-edit--replace-with-ref beg end :val "threshold")

;;; Code:

(require 'cl-lib)
(require 'duckdb-query-parse)

;;;; Reference Format Constants

(defconst duckdb-query-edit--ref-types '(:val :sql :data)
  "Reference types supported by extraction commands.

Does not include :org since org references resolve from buffer
context rather than inline bindings.")

(defconst duckdb-query-edit--ref-format-alist
  '((:val . "@val:%s")
    (:sql . "@sql:%s")
    (:data . "@data:%s"))
  "Alist mapping reference types to format strings for substitution.")

;;;; Primitive: Locate Parameter

(defun duckdb-query-edit--find-param (form-beg form-end keyword)
  "Find KEYWORD parameter in form between FORM-BEG and FORM-END.

KEYWORD is a keyword symbol (e.g., :val, :sql, :data).

Return plist with :key-beg, :key-end, :val-beg, :val-end if found.
Return nil if KEYWORD is not present in the form.

Uses `duckdb-query--parse-params' for structural extraction."
  (let ((params (duckdb-query--parse-params form-beg form-end)))
    (cl-find-if (lambda (p) (eq (plist-get p :key) keyword)) params)))

;;;; Primitive: Locate Parameter

(defun duckdb-query-edit--insert-new-param (form-end keyword name value)
  "Insert new KEYWORD parameter with binding (NAME . VALUE).

FORM-END is position after closing paren of form.
KEYWORD is :val, :sql, or :data.
NAME is binding name string.
VALUE is Elisp literal string.

Insert before the closing parenthesis of the `duckdb-query' form.
Return position after inserted text."
  (save-excursion
    (goto-char (1- form-end))
    (let* ((form-col (save-excursion
                       (backward-up-list 1)
                       (current-column)))
           (indent (make-string (+ form-col 14) ?\s)))
      (insert "\n" indent (symbol-name keyword)
              " '((" name " . " value "))")
      (point))))

(defun duckdb-query-edit--find-last-binding-end (val-beg val-end)
  "Find position after last binding entry in alist between VAL-BEG and VAL-END.

Navigate into the quoted alist structure and locate the closing
parenthesis of the last cons cell.  Return position after that
paren, or nil if structure is not recognized.

VAL-BEG is position of the quote or backquote before the alist.
VAL-END is position after the entire value form."
  (ignore val-end)
  (save-excursion
    (goto-char val-beg)
    ;; Skip quote/backquote
    (when (memq (char-after) '(?' ?`))
      (forward-char 1))
    ;; Now at opening paren of alist
    (when (eq (char-after) ?\()
      (let ((list-start (point))
            (list-end (save-excursion
                        (when (duckdb-query--forward-sexp-safe)
                          (point)))))
        (when list-end
          ;; Walk forward through the alist entries to find last one
          (goto-char list-start)
          (forward-char 1)
          (let ((last-entry-end nil))
            (while (< (point) (1- list-end))
              (duckdb-query--skip-whitespace-and-comments)
              ;; Skip unquote markers
              (when (eq (char-after) ?,)
                (forward-char 1)
                (when (eq (char-after) ?@)
                  (forward-char 1)))
              (duckdb-query--skip-whitespace-and-comments)
              (when (and (< (point) (1- list-end))
                         (duckdb-query--forward-sexp-safe))
                (setq last-entry-end (point))))
            last-entry-end))))))

(defun duckdb-query-edit--param-indent (param)
  "Compute indentation string for bindings inside PARAM.

PARAM is plist from `duckdb-query-edit--find-param'.
Return whitespace string matching the indentation of existing
bindings, or reasonable default based on parameter position."
  (save-excursion
    (goto-char (plist-get param :val-beg))
    ;; Skip quote/backquote and opening paren to find first binding
    (skip-chars-forward "'`(")
    (if (eq (char-after) ?\()
        ;; At first binding's opening paren
        (make-string (current-column) ?\s)
      ;; Fallback: indent relative to keyword
      (make-string (+ (save-excursion
                         (goto-char (plist-get param :key-beg))
                         (current-column))
                       1)
                   ?\s))))




(provide 'duckdb-query-edit)

;;; duckdb-query-edit.el ends here
