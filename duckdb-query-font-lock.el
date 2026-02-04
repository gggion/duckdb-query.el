;;; duckdb-query-font-lock.el --- Font-lock for duckdb-query SQL strings -*- lexical-binding: t; -*-

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

;; Provides font-lock highlighting for @type:name references within
;; duckdb-query SQL strings.
;;
;; Basic usage:
;;
;;     (add-hook 'emacs-lisp-mode-hook #'duckdb-query-font-lock-mode)
;;
;; For Org src blocks, the above hook is sufficient since Org's native
;; fontification uses `emacs-lisp-mode' internally.  Optionally, also
;; enable in Org buffers directly:
;;
;;     (add-hook 'org-mode-hook #'duckdb-query-font-lock-mode)
;;
;; Select a highlighting preset interactively:
;;
;;     M-x duckdb-query-font-lock-select-preset
;;
;; Highlighted reference types:
;; - @org:table-name - Org table references
;; - @data:name - Elisp data bindings
;; - @val:name - SQL variable literals

;;; Code:

(require 'font-lock)

(defgroup duckdb-query-font-lock nil
  "Font-lock support for duckdb-query SQL strings."
  :group 'duckdb-query
  :prefix "duckdb-query-font-lock-")

;;;; Faces

(defface duckdb-query-reference-prefix-face
  '((t :inherit shadow))
  "Face for @type: prefix in duckdb-query references."
  :group 'duckdb-query-font-lock)

(defface duckdb-query-reference-name-face
  '((t :inherit font-lock-constant-face :weight bold))
  "Face for the name portion of @type:name references."
  :group 'duckdb-query-font-lock)

;;;; Presets

(defvar duckdb-query-font-lock-presets
  '((shadow-bold-constant
     :prefix (:inherit shadow)
     :name (:inherit font-lock-constant-face :weight bold))

    (shadow-bold-variable
     :prefix (:inherit shadow)
     :name (:inherit font-lock-variable-name-face :weight bold))

    (keyword-variable
     :prefix (:inherit font-lock-keyword-face)
     :name (:inherit font-lock-variable-name-face :weight bold))

    (uniform-type
     :prefix (:inherit font-lock-type-face)
     :name (:inherit font-lock-type-face))

    (uniform-keyword
     :prefix (:inherit font-lock-keyword-face)
     :name (:inherit font-lock-keyword-face))

    (italic-shadow-bold-constant
     :prefix (:inherit shadow :slant italic)
     :name (:inherit font-lock-constant-face :weight bold))

    (light-extra-bold
     :prefix (:inherit font-lock-constant-face :weight light)
     :name (:inherit font-lock-constant-face :weight extra-bold))

    (shadow-completions
     :prefix (:inherit shadow)
     :name (:inherit completions-common-part :weight bold))

    (italic-shadow-bold-underline
     :prefix (:inherit shadow :slant italic)
     :name (:inherit font-lock-constant-face :weight bold :underline t)))
  "Alist of highlighting presets for duckdb-query references.
Each entry is (NAME :prefix PLIST :name PLIST).")

(defcustom duckdb-query-font-lock-preset 'shadow-bold-constant
  "Current highlighting preset for duckdb-query references.
Use `duckdb-query-font-lock-select-preset' to change interactively."
  :type `(choice ,@(mapcar (lambda (p) `(const ,(car p)))
                           duckdb-query-font-lock-presets))
  :group 'duckdb-query-font-lock
  :set (lambda (sym val)
         (set-default sym val)
         (when (fboundp 'duckdb-query-font-lock-apply-preset)
           (duckdb-query-font-lock-apply-preset val))))

;;;; Preset Application

(defun duckdb-query-font-lock-apply-preset (preset)
  "Apply PRESET to duckdb-query reference faces.
PRESET is a symbol naming an entry in `duckdb-query-font-lock-presets'."
  (let ((entry (assq preset duckdb-query-font-lock-presets)))
    (unless entry
      (error "Unknown preset: %s" preset))
    (let ((prefix-attrs (plist-get (cdr entry) :prefix))
          (name-attrs (plist-get (cdr entry) :name)))
      ;; Reset faces
      (set-face-attribute 'duckdb-query-reference-prefix-face nil
                          :inherit nil :weight 'normal :slant 'normal
                          :underline nil :foreground 'unspecified
                          :background 'unspecified :box nil)
      (set-face-attribute 'duckdb-query-reference-name-face nil
                          :inherit nil :weight 'normal :slant 'normal
                          :underline nil :foreground 'unspecified
                          :background 'unspecified :box nil)
      ;; Apply new attributes
      (apply #'set-face-attribute 'duckdb-query-reference-prefix-face nil
             prefix-attrs)
      (apply #'set-face-attribute 'duckdb-query-reference-name-face nil
             name-attrs)
      ;; Refontify buffers with mode enabled
      (dolist (buf (buffer-list))
        (with-current-buffer buf
          (when (bound-and-true-p duckdb-query-font-lock-mode)
            (font-lock-flush)))))))

;;;; Preset Selection

(defvar duckdb-query-font-lock--select-history nil
  "Minibuffer history for `duckdb-query-font-lock-select-preset'.")

(defvar duckdb-query-font-lock--saved-preset nil
  "Saved preset for restoration on cancel.")

(defvar duckdb-query-font-lock--last-previewed nil
  "Last previewed preset to avoid redundant applications.")

(defun duckdb-query-font-lock--preview-preset ()
  "Preview the currently selected preset in minibuffer."
  (when (minibufferp)
    (when-let* ((candidate (or
                            ;; Vertico
                            (and (bound-and-true-p vertico--index)
                                 (bound-and-true-p vertico--candidates)
                                 (>= vertico--index 0)
                                 (nth vertico--index vertico--candidates))
                            ;; Icomplete
                            (and (bound-and-true-p icomplete-mode)
                                 (car completion-all-sorted-completions))
                            ;; Standard completion
                            (let ((c (minibuffer-contents-no-properties)))
                              (when (assq (intern-soft c)
                                          duckdb-query-font-lock-presets)
                                c))))
                (preset (intern-soft candidate))
                ((assq preset duckdb-query-font-lock-presets))
                ((not (eq preset duckdb-query-font-lock--last-previewed))))
      (setq duckdb-query-font-lock--last-previewed preset)
      (duckdb-query-font-lock-apply-preset preset))))

(defun duckdb-query-font-lock--setup-preview ()
  "Setup preview hooks in minibuffer."
  (setq duckdb-query-font-lock--last-previewed nil)
  (add-hook 'post-command-hook #'duckdb-query-font-lock--preview-preset nil t))

;;;###autoload
(defun duckdb-query-font-lock-select-preset (preset)
  "Select a highlighting PRESET with live preview.
Interactively, show completion with preview as you navigate
candidates.  Works with `vertico', `icomplete', `fido-mode', and
standard completion.

PRESET is a symbol from `duckdb-query-font-lock-presets'."
  (interactive
   (let ((duckdb-query-font-lock--saved-preset duckdb-query-font-lock-preset)
         (selected nil))
     (minibuffer-with-setup-hook #'duckdb-query-font-lock--setup-preview
       (unwind-protect
           (setq selected
                 (intern
                  (completing-read
                   (format-prompt "Preset" duckdb-query-font-lock-preset)
                   (mapcar (lambda (p) (symbol-name (car p)))
                           duckdb-query-font-lock-presets)
                   nil t nil
                   'duckdb-query-font-lock--select-history
                   (symbol-name duckdb-query-font-lock-preset))))
         ;; On C-g, restore saved preset
         (unless selected
           (duckdb-query-font-lock-apply-preset
            duckdb-query-font-lock--saved-preset))))
     (list selected)))
  (when preset
    (setq duckdb-query-font-lock-preset preset)
    (duckdb-query-font-lock-apply-preset preset)
    (message "Applied preset: %s" preset)))

;;;; Font-lock Implementation

(defconst duckdb-query-font-lock--query-functions
  '(duckdb-query)
  "`duckdb-query' Functions whose first string argument is a SQL query.")

(defconst duckdb-query-font-lock--reference-regexp
  (rx (group "@" (or "org" "data" "val" "sql") ":")
      (group (any "a-zA-Z_") (* (any "a-zA-Z0-9_:/-"))))
  "Regexp matching @type:name references.
Group 1: @type: prefix.
Group 2: name.

Reference types:
- org:  Org table references via `org-babel-ref-resolve'
- data: Elisp data bindings from :data parameter
- val:  SQL variable literals from :val parameter
- sql:  SQL string fragments for query construction")

(defun duckdb-query-font-lock--in-quoted-context-p ()
  "Return non-nil if point is inside a quoted form."
  (save-excursion
    (catch 'quoted
      (while (let ((paren-pos (nth 1 (syntax-ppss))))
               (when paren-pos
                 (goto-char paren-pos)
                 (when (memq (char-before) '(?\' ?\`))
                   (throw 'quoted t))
                 (when (looking-at "([ \t\n]*quote\\_>")
                   (throw 'quoted t))
                 t)))
      nil)))

(defun duckdb-query-font-lock--in-duckdb-query-string-p (pos)
  "Return non-nil if POS is inside a duckdb-query SQL string."
  (save-excursion
    (goto-char pos)
    (let ((ppss (syntax-ppss)))
      (when (and (nth 3 ppss)
                 (not (nth 4 ppss)))
        (let ((string-start (nth 8 ppss)))
          (goto-char string-start)
          (unless (duckdb-query-font-lock--in-quoted-context-p)
            (let ((paren-pos (nth 1 (syntax-ppss))))
              (when paren-pos
                (goto-char paren-pos)
                (forward-char 1)
                (skip-chars-forward " \t\n")
                (and (looking-at "\\_<\\(duckdb-query[-a-z]*\\)\\_>")
                     (memq (intern (match-string 1))
                           duckdb-query-font-lock--query-functions)
                     (progn
                       (goto-char (match-end 0))
                       (skip-chars-forward " \t\n")
                       (= (point) string-start)))))))))))

(defun duckdb-query-font-lock--fontify-references (limit)
  "Fontify @type:name references up to LIMIT."
  (let ((found nil))
    (while (re-search-forward duckdb-query-font-lock--reference-regexp limit t)
      (let ((prefix-beg (match-beginning 1))
            (prefix-end (match-end 1))
            (name-beg (match-beginning 2))
            (name-end (match-end 2)))
        (when (duckdb-query-font-lock--in-duckdb-query-string-p prefix-beg)
          (setq found t)
          (add-face-text-property prefix-beg prefix-end
                                  'duckdb-query-reference-prefix-face)
          (add-face-text-property name-beg name-end
                                  'duckdb-query-reference-name-face))))
    found))

(defconst duckdb-query-font-lock-keywords
  '((duckdb-query-font-lock--fontify-references))
  "Font-lock keywords for duckdb-query references.")

;;;; Minor Mode

;;;###autoload
(define-minor-mode duckdb-query-font-lock-mode
  "Minor mode for highlighting @type:name references in duckdb-query strings.

When enabled, references like @org:table, @data:name, @val:value,
and @expr:query are highlighted within SQL string arguments to
`duckdb-query' and related functions.

To enable globally:

    (add-hook \\='emacs-lisp-mode-hook #\\='duckdb-query-font-lock-mode)

Use `duckdb-query-font-lock-select-preset' to change highlighting
style interactively with live preview."
  :lighter " DQ"
  :group 'duckdb-query-font-lock
  (if duckdb-query-font-lock-mode
      (progn
        (duckdb-query-font-lock-apply-preset duckdb-query-font-lock-preset)
        (font-lock-add-keywords nil duckdb-query-font-lock-keywords 'append))
    (font-lock-remove-keywords nil duckdb-query-font-lock-keywords))
  (font-lock-flush))

(provide 'duckdb-query-font-lock)

;;; duckdb-query-font-lock.el ends here
