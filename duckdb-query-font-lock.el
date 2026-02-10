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

;; Highlight @type:name references in `duckdb-query' SQL strings.
;;
;; Basic usage:
;;
;;     (add-hook 'emacs-lisp-mode-hook #'duckdb-query-font-lock-mode)
;;
;; Select highlighting style interactively:
;;
;;     M-x duckdb-query-font-lock-select-preset
;;
;; Highlighted reference types:
;; - @org:name   Org table references
;; - @data:name  Elisp data bindings
;; - @val:name   SQL variable literals
;; - @sql:name   SQL fragment substitution
;;
;; Invalid references (undefined bindings or invalid context) use
;; `duckdb-query-reference-invalid-face' to signal resolution failure.
;;
;; Uses `duckdb-query-parse.el' for structural parsing.
;; Also see `duckdb-query' for reference syntax documentation.

;;; Code:

(require 'font-lock)
(require 'cl-lib)
(require 'duckdb-query-parse)

;; Defined by font-lock, bound when `font-lock-extend-region-functions' run.
(defvar font-lock-beg)
(defvar font-lock-end)

;; Defined by duckdb-query-parse.el, used by find-form function.
(defvar duckdb-query--query-function-regexp)

(defgroup duckdb-query-font-lock nil
  "Font-lock support for `duckdb-query' SQL strings."
  :group 'duckdb-query
  :prefix "duckdb-query-font-lock-")

;;;; Faces

(defface duckdb-query-reference-prefix-face
  '((t :inherit shadow))
  "Face for @type: prefix in `duckdb-query' references.

Applied to the prefix portion of valid references.

Customized via `duckdb-query-font-lock-preset' or directly.
Also see `duckdb-query-reference-name-face' for the name portion."
  :group 'duckdb-query-font-lock
  :package-version '(duckdb-query . "0.7.0"))

(defface duckdb-query-reference-name-face
  '((t :inherit font-lock-constant-face :weight bold))
  "Face for the name portion of @type:name references.

Applied to the name portion of valid references.

Customized via `duckdb-query-font-lock-preset' or directly.
Also see `duckdb-query-reference-prefix-face' for the prefix portion."
  :group 'duckdb-query-font-lock
  :package-version '(duckdb-query . "0.7.0"))

(defface duckdb-query-reference-invalid-face
  '((t :inherit warning :weight bold))
  "Face for invalid references.

Applied to references that cannot be resolved:
- @sql: or @org: inside (sql ...) wrappers in :val bindings
- References to undefined binding names

Also see `duckdb-query--validate-reference' for validation rules."
  :group 'duckdb-query-font-lock
  :package-version '(duckdb-query . "0.7.0"))

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
  "Alist of highlighting presets for `duckdb-query' references.

Each entry is (NAME :prefix FACE-ATTRS :name FACE-ATTRS) where
FACE-ATTRS is a plist of face attributes.

Applied by `duckdb-query-font-lock-apply-preset'.
Selected via `duckdb-query-font-lock-select-preset'.")

(defcustom duckdb-query-font-lock-preset 'shadow-bold-constant
  "Current highlighting preset for `duckdb-query' references.

Controls appearance of `duckdb-query-reference-prefix-face' and
`duckdb-query-reference-name-face'.

Use `duckdb-query-font-lock-select-preset' to change interactively
with live preview."
  :type `(choice ,@(mapcar (lambda (p) `(const ,(car p)))
                           duckdb-query-font-lock-presets))
  :group 'duckdb-query-font-lock
  :set (lambda (sym val)
         (set-default sym val)
         (when (fboundp 'duckdb-query-font-lock-apply-preset)
           (duckdb-query-font-lock-apply-preset val)))
  :package-version '(duckdb-query . "0.7.0"))

(defcustom duckdb-query-font-lock-in-org-src-blocks t
  "Whether to highlight references in `org-mode' src block fontification.

When non-nil, `duckdb-query-font-lock-mode' installs font-lock
keywords in org's internal fontification buffers with minimal
overhead, skipping preset application, region extension hooks,
and contextual JIT lock setup.

When nil, font-lock keywords are not installed in org fontification
buffers.  References still highlight in dedicated src edit buffers
opened via \\[org-edit-special].

Has no effect unless `duckdb-query-font-lock-mode' is on
`emacs-lisp-mode-hook'.

Also see `duckdb-query-font-lock-mode'."
  :type 'boolean
  :group 'duckdb-query-font-lock
  :package-version '(duckdb-query . "0.8.0"))

(defcustom duckdb-query-font-lock-max-buffer-size nil
  "Maximum buffer size in bytes for reference fontification.

When non-nil and buffer size exceeds this value, fontification
behavior depends on context:

In `emacs-lisp-mode' buffers, `duckdb-query-font-lock--propertize'
skips propertization entirely.

In org src fontification buffers, the mode inhibits activation
regardless of `duckdb-query-font-lock-in-org-src-blocks'.  The
buffer size checked is the org buffer's size, not the fontification
buffer's size.

When nil, fontify regardless of buffer size.

Also see `duckdb-query-complete-max-buffer-size'."
  :type '(choice (const :tag "No limit" nil)
                 (integer :tag "Maximum bytes"))
  :group 'duckdb-query-font-lock
  :package-version '(duckdb-query . "0.8.0"))

;;;; Preset Application

(defun duckdb-query-font-lock-apply-preset (preset)
  "Apply PRESET to `duckdb-query' reference faces.

PRESET is a symbol naming an entry in `duckdb-query-font-lock-presets'.

Reset face attributes before applying new values.  Trigger
`font-lock-flush' in all buffers with `duckdb-query-font-lock-mode'.

Called by `duckdb-query-font-lock-select-preset'.
Also see the :set function of `duckdb-query-font-lock-preset'."
  (let ((entry (assq preset duckdb-query-font-lock-presets)))
    (unless entry
      (error "Unknown preset: %s" preset))
    (let ((prefix-attrs (plist-get (cdr entry) :prefix))
          (name-attrs (plist-get (cdr entry) :name)))
      ;; Reset faces to default state
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
  "Saved preset for restoration on cancel.

Bound by `duckdb-query-font-lock-select-preset' before entering
minibuffer.  Restored if user cancels with \\[keyboard-quit].")

(defvar duckdb-query-font-lock--last-previewed nil
  "Last previewed preset to avoid redundant applications.

Compared against current candidate by
`duckdb-query-font-lock--preview-preset'.")

(defun duckdb-query-font-lock--preview-preset ()
  "Preview the currently selected preset in minibuffer.

Extract current candidate from vertico, icomplete, or standard
completion and apply it via `duckdb-query-font-lock-apply-preset'.

Called by `post-command-hook' in minibuffer during preset selection."
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
  "Install preview hook in minibuffer.

Add `duckdb-query-font-lock--preview-preset' to `post-command-hook'
for live preview during `duckdb-query-font-lock-select-preset'."
  (setq duckdb-query-font-lock--last-previewed nil)
  (add-hook 'post-command-hook #'duckdb-query-font-lock--preview-preset nil t))

;;;###autoload
(defun duckdb-query-font-lock-select-preset (preset)
  "Select a highlighting PRESET with live preview.

Interactively, show completion with preview as you navigate
candidates.  Works with `vertico', `icomplete', `fido-mode', and
standard completion.

PRESET is a symbol from `duckdb-query-font-lock-presets'.

On \\[keyboard-quit], restore the previous preset.

Also see `duckdb-query-font-lock-preset' for programmatic access."
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

;;;; Fontification Engine
;;;;; Face Application

(defun duckdb-query-font-lock--compute-prefix-end (ref-beg ref-type)
  "Compute end position of @type: prefix starting at REF-BEG.

REF-TYPE is the reference type symbol (sql, data, val, org).
Returns position after the colon in the @type: prefix.

Called by `duckdb-query-font-lock--apply-reference-face'."
  (+ ref-beg 1 (length (symbol-name ref-type)) 1))

(defun duckdb-query-font-lock--apply-reference-face (ref invalid-p)
  "Apply face to reference REF.

REF is a plist with :type, :name, :beg, :end keys from
`duckdb-query--find-references-in-string'.
INVALID-P is non-nil if the reference failed validation.

Apply `duckdb-query-reference-invalid-face' to entire reference
when INVALID-P is non-nil.  Otherwise apply prefix and name faces
to their respective portions.

Called by `duckdb-query-font-lock--fontify-form'."
  (let ((ref-beg (plist-get ref :beg))
        (ref-end (plist-get ref :end))
        (ref-type (plist-get ref :type)))
    (if invalid-p
        (font-lock-prepend-text-property
         ref-beg ref-end 'face 'duckdb-query-reference-invalid-face)
      (let ((prefix-end (duckdb-query-font-lock--compute-prefix-end
                         ref-beg ref-type)))
        (font-lock-prepend-text-property
         ref-beg prefix-end 'face 'duckdb-query-reference-prefix-face)
        (font-lock-prepend-text-property
         prefix-end ref-end 'face 'duckdb-query-reference-name-face)))))

;;;;; Form Fontification

(defun duckdb-query-font-lock--fontify-form (parse-result)
  "Fontify all references in PARSE-RESULT.

PARSE-RESULT is a `duckdb-query-parse-result' struct from
`duckdb-query--parse-at-point'.

Validate all references via `duckdb-query--validate-all-references'
and apply appropriate faces based on validity status.

Called by `duckdb-query-font-lock--propertize'."
  (let* ((references (duckdb-query-parse-result-references parse-result))
         (invalid-refs (duckdb-query--validate-all-references parse-result))
         (invalid-positions (make-hash-table :test 'eq)))
    (dolist (entry invalid-refs)
      (puthash (plist-get (car entry) :beg) t invalid-positions))
    (dolist (ref references)
      (duckdb-query-font-lock--apply-reference-face
       ref (gethash (plist-get ref :beg) invalid-positions)))))

;;;;; Form Detection

(defun duckdb-query-font-lock--find-form-start (pos)
  "Find start of `duckdb-query' family form containing POS.

Walk backward from POS through list structure looking for an
enclosing call matching `duckdb-query--query-function-regexp'.
Handle the case where POS is inside a string by first moving
to string start.

Return form start position, or nil if POS is not inside a
recognized form.

Called by `duckdb-query-font-lock--propertize'.
Called by `duckdb-query-font-lock--extend-region'."
  (save-excursion
    (goto-char pos)
    (let ((ppss (syntax-ppss)))
      (when (nth 3 ppss)
        (goto-char (nth 8 ppss))))
    (let ((found nil))
      (while (and (not found)
                  (condition-case nil
                      (progn (backward-up-list 1) t)
                    (scan-error nil)))
        (when (looking-at duckdb-query--query-function-regexp)
          (setq found (point))))
      found)))

;;;;; Region-Aware Fontification

(defun duckdb-query-font-lock--propertize (end)
  "Fontify `duckdb-query' references between point and END.

Search for `duckdb-query' family forms in the region, parse each
form, validate references, and apply faces.

When the region starts inside an existing form, find and fontify
that form first before searching forward.

Skip fontification when buffer size exceeds
`duckdb-query-font-lock-max-buffer-size'.  This guard protects
standalone `emacs-lisp-mode' buffers; org fontification buffers
are guarded at mode activation time.

Return nil to tell font-lock we applied faces ourselves.

Uses `duckdb-query--parse-at-point' for structural parsing.
Uses `duckdb-query-font-lock--fontify-form' for face application."
  (when (or (null duckdb-query-font-lock-max-buffer-size)
            (<= (buffer-size) duckdb-query-font-lock-max-buffer-size))
    (let ((start (point))
          (fontified-forms (make-hash-table :test 'eq)))
      ;; Handle region starting inside a form
      (when-let ((form-start (duckdb-query-font-lock--find-form-start start)))
        (unless (gethash form-start fontified-forms)
          (save-excursion
            (goto-char form-start)
            (when-let ((parse-result (duckdb-query--parse-at-point)))
              (duckdb-query-font-lock--fontify-form parse-result)
              (puthash form-start t fontified-forms)
              (setq start (max start
                               (duckdb-query-parse-result-form-end
                                parse-result)))))))
      ;; Search forward for additional forms
      (goto-char start)
      (while (re-search-forward duckdb-query--query-function-regexp end t)
        (let ((form-start (match-beginning 0)))
          (goto-char form-start)
          (unless (or (duckdb-query--in-string-or-comment-p)
                      (gethash form-start fontified-forms))
            (when-let ((parse-result (duckdb-query--parse-at-point)))
              (duckdb-query-font-lock--fontify-form parse-result)
              (puthash form-start t fontified-forms)
              (goto-char (duckdb-query-parse-result-form-end parse-result))))
          (when (<= (point) form-start)
            (goto-char (1+ form-start)))))))
  nil)

(defconst duckdb-query-font-lock--keywords
  '(duckdb-query-font-lock--propertize)
  "Font-lock keywords for `duckdb-query' references.

Installed by `duckdb-query-font-lock-mode'.")

;;;;; Region Extension

(defun duckdb-query-font-lock--extend-region ()
  "Extend font-lock region to include complete `duckdb-query' forms.

When `font-lock-beg' or `font-lock-end' falls inside a form,
extend the region to include the entire form.  This ensures
references are fontified correctly when JIT lock requests
fontification of partial regions.

Return non-nil if region was extended.

Uses `duckdb-query-font-lock--find-form-start' for form detection.
Installed on `font-lock-extend-region-functions' by
`duckdb-query-font-lock-mode'."
  (let ((extended nil))
    (when-let ((form-start (duckdb-query-font-lock--find-form-start
                            font-lock-beg)))
      (when (< form-start font-lock-beg)
        (setq font-lock-beg form-start
              extended t)))
    (when-let ((form-start (duckdb-query-font-lock--find-form-start
                            font-lock-end)))
      (save-excursion
        (goto-char form-start)
        (when-let ((parse-result (duckdb-query--parse-at-point)))
          (let ((form-end (duckdb-query-parse-result-form-end parse-result)))
            (when (> form-end font-lock-end)
              (setq font-lock-end form-end
                    extended t))))))
    extended))

(defun duckdb-query-font-lock--in-org-fontification-p ()
  "Return non-nil if current buffer is an org src fontification buffer.

Org creates temporary buffers named \" org-src-fontification:*\"
for src block font-lock.  These buffers run language major modes
but benefit from lightweight mode activation that skips expensive
setup like preset application and region extension hooks.

Called by `duckdb-query-font-lock-mode' to select activation path."
  (string-prefix-p " org-src-fontification:"
                    (buffer-name)))

(defun duckdb-query-font-lock--org-buffer-too-large-p ()
  "Return non-nil if the org buffer being fontified exceeds size threshold.

In org fontification buffers, `org-src--source-type' or the buffer
from which fontification was requested may not be directly accessible.
Use `buffer-local-value' on buffers matching the org file, or check
all `org-mode' buffers.

Falls back to checking if any `org-mode' buffer exceeds the threshold,
which is conservative but safe."
  (and duckdb-query-font-lock-max-buffer-size
       (cl-some (lambda (buf)
                  (and (buffer-live-p buf)
                       (with-current-buffer buf
                         (derived-mode-p 'org-mode))
                       (> (buffer-size buf)
                          duckdb-query-font-lock-max-buffer-size)))
                (buffer-list))))

;;;; Minor Mode

;;;###autoload
(define-minor-mode duckdb-query-font-lock-mode
  "Highlight @type:name references in `duckdb-query' forms.

When enabled, references like @org:table, @data:name, @val:value,
and @sql:fragment are highlighted within SQL string arguments to
`duckdb-query' and related functions.

References are highlighted based on structural parsing by
`duckdb-query-parse.el'.  Invalid references (undefined bindings
or invalid context like @sql: in (sql ...) wrappers) are
highlighted with `duckdb-query-reference-invalid-face'.

In org src fontification buffers, activation is lightweight: only
font-lock keywords are installed, skipping preset application and
region extension hooks.  Controlled by
`duckdb-query-font-lock-in-org-src-blocks'.  Inhibited entirely
when `duckdb-query-font-lock-max-buffer-size' is set and the org
buffer exceeds the threshold.

To enable globally:

    (add-hook \\='emacs-lisp-mode-hook #\\='duckdb-query-font-lock-mode)

Use `duckdb-query-font-lock-select-preset' to change highlighting
style interactively with live preview.

Also see `duckdb-query' for reference syntax documentation."
  :lighter " DQ"
  :group 'duckdb-query-font-lock
  (cond
   ;; Org fontification buffer
   ((and duckdb-query-font-lock-mode
         (duckdb-query-font-lock--in-org-fontification-p))
    (cond
     ;; Org buffer too large: inhibit
     ((duckdb-query-font-lock--org-buffer-too-large-p)
      (setq duckdb-query-font-lock-mode nil))
     ;; Lightweight activation enabled
     (duckdb-query-font-lock-in-org-src-blocks
      (font-lock-add-keywords nil duckdb-query-font-lock--keywords 'append))
     ;; Inhibit entirely
     (t
      (setq duckdb-query-font-lock-mode nil))))
   ;; Normal enable
   (duckdb-query-font-lock-mode
    (duckdb-query-font-lock-apply-preset duckdb-query-font-lock-preset)
    (font-lock-add-keywords nil duckdb-query-font-lock--keywords 'append)
    (setq-local jit-lock-contextually t)
    (add-hook 'font-lock-extend-region-functions
              #'duckdb-query-font-lock--extend-region nil t)
    (font-lock-flush))
   ;; Disable
   (t
    (font-lock-remove-keywords nil duckdb-query-font-lock--keywords)
    (remove-hook 'font-lock-extend-region-functions
                 #'duckdb-query-font-lock--extend-region t)
    (font-lock-flush))))

(provide 'duckdb-query-font-lock)

;;; duckdb-query-font-lock.el ends here
