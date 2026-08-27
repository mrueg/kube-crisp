# The Pagila tutorial in twenty seconds: a thousand films that live in
# PostgreSQL, listed and read as ordinary Kubernetes objects.
#
# Regenerate with:
#
#     ./hack/e2e-up.sh          # a kind cluster with Pagila loaded
#     vhs docs/demo/pagila.tape # from the repository root
#
# Hack is used because it is the monospace font this was recorded with; VHS
# falls back to its own if you do not have it.

Output docs/demo/pagila.gif

Set Shell bash
Set FontFamily "Hack"
Set FontSize 14
Set Width 1180
Set Height 800
Set Padding 20
Set TypingSpeed 40ms
Set Framerate 24

# Off camera: the kubeconfig e2e-up.sh writes, and a prompt that is this
# recording's rather than whichever one the machine happens to have.
Hide
Type "export KUBECONFIG=$PWD/hack/.e2e-kubeconfig" Enter
Type "PS1='$ ' PS2='' PROMPT_COMMAND=" Enter
Type "clear" Enter
Show

Sleep 1s

# There is no CRD behind this and no controller writing objects. Every row is
# read from Pagila when the request is served.
Type "kubectl get films | head -6" Sleep 400ms Enter
Sleep 3.5s

Type "kubectl get films --no-headers | wc -l" Sleep 400ms Enter
Sleep 2.5s

# Labels come from columns, so the catalogue slices like any other collection.
Type "kubectl get films -l pagila.example.com/category=Documentary | head -4" Sleep 400ms Enter
Sleep 3.5s

Hide
Type "clear" Enter
Show
Sleep 500ms

# One film, in full. The labels are columns, spec.actors and spec.categories
# are the two join tables, and status is what PostgreSQL computes.
Type "kubectl get film academy-dinosaur -o yaml" Sleep 400ms Enter
Sleep 6s
